package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"math/bits"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/pterm/pterm"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	tunnelConfig "github.com/ton-blockchain/adnl-tunnel/config"
	"github.com/ton-blockchain/adnl-tunnel/tunnel"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/adnl"
	adnlAddress "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-storage-provider/pkg/contract"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
	"github.com/xssnick/tonutils-storage/api"
	"github.com/xssnick/tonutils-storage/config"
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/provider"
	"github.com/xssnick/tonutils-storage/storage"

	_ "net/http/pprof"
)

var (
	API                 = flag.String("api", "", "HTTP API listen address")
	CredentialsLogin    = flag.String("api-login", "", "HTTP API credentials login")
	CredentialsPassword = flag.String("api-password", "", "HTTP API credentials password")
	DBPath              = flag.String("db", "tonutils-storage-db", "Path to db folder")
	Verbosity           = flag.Int("verbosity", 2, "Debug logs")
	IsDaemon            = flag.Bool("daemon", false, "Daemon mode, no command line input")
	NetworkConfigPath   = flag.String("network-config", "", "Network config path to load from disk")
	Version             = flag.Bool("version", false, "Show version and exit")
	NoVerify            = flag.Bool("no-verify", false, "Skip bags files integrity verification on startup")
	NoRemove            = flag.Bool("no-remove", false, "Do not remove any files even on bag deletion or integrity failure")
	ListenThreads       = flag.Int("threads", 0, "Listen threads")
	CachedFD            = flag.Int("fd-cache-limit", 800, "Set max open files limit")
	ForcePieceSize      = flag.Int("force-piece-size", 0, "Set piece size for bag creation, automatically chosen when flag is not set")
	EnableTunnel        = flag.Bool("enable-tunnel", false, "Enable tunnel mode, to host files with no public ip (should be configured first)")
	DHTParallelism      = flag.Int("dht-parallelism", 12, "Max parallel threads to search/update dht records of bags")
	PprofEnableAddr     = flag.String("pprof-addr", "", "Enable pprof HTTP server for performance profiling on specified addr")
)

var GitCommit string

var Storage *db.Storage
var Provider *provider.Client
var Connector storage.NetConnector
var Config *config.Config

func main() {
	flag.Parse()

	if *Version {
		println("Build version: " + GitCommit)
		os.Exit(0)
	}

	storage.Logger = func(v ...any) {}
	adnl.Logger = func(v ...any) {}
	dht.Logger = func(v ...any) {}
	provider.Logger = func(...any) {}

	if *Verbosity > 13 {
		*Verbosity = 13
	}

	switch *Verbosity {
	case 13:
		adnl.Logger = log.Logger.Println
		dht.Logger = log.Logger.Println
		fallthrough
	case 12:
		rldp.Logger = log.Logger.Println
		fallthrough
	case 11:
		storage.Logger = log.Logger.Println
		provider.Logger = log.Logger.Println
	}

	pterm.DefaultBox.WithBoxStyle(pterm.NewStyle(pterm.FgLightBlue)).Println(pterm.LightWhite("    Tonutils Storage   "))
	pterm.Info.Println("Version:", GitCommit)

	if *CachedFD > 0 {
		db.CachedFDLimit = *CachedFD
	}

	if *DBPath == "" {
		pterm.Error.Println("DB path should be specified with -db flag")
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*DBPath)
	if err != nil {
		pterm.Error.Println("Failed to load config:", err.Error())
		os.Exit(1)
	}
	Config = cfg

	if *PprofEnableAddr != "" {
		go func() {
			pterm.Info.Println("Starting pprof HTTP server on", *PprofEnableAddr)
			err := http.ListenAndServe(*PprofEnableAddr, nil)
			if err != nil {
				pterm.Fatal.Println("Failed to start pprof server:", err.Error())
			}
		}()
	}

	closerCtx, stop := context.WithCancel(context.Background())
	defer stop()

	ldb, err := leveldb.OpenFile(*DBPath+"/db", &opt.Options{
		WriteBuffer: 64 << 20,
	})
	if err != nil {
		pterm.Error.Println("Failed to load db:", err.Error())
		os.Exit(1)
	}
	defer ldb.Close()

	var ip net.IP
	var port uint16
	if cfg.ExternalIP != "" {
		ip = net.ParseIP(cfg.ExternalIP)
		if ip == nil {
			pterm.Error.Println("External ip is invalid")
			os.Exit(1)
		}
	}

	addr, err := netip.ParseAddrPort(cfg.ListenAddr)
	if err != nil {
		pterm.Error.Println("Listen addr is invalid")
		os.Exit(1)
	}
	port = addr.Port()

	var lsCfg *liteclient.GlobalConfig
	if *NetworkConfigPath != "" {
		lsCfg, err = liteclient.GetConfigFromFile(*NetworkConfigPath)
		if err != nil {
			pterm.Error.Println("Failed to load ton network config from file:", err.Error())
			os.Exit(1)
		}
	} else {
		lsCfg, err = liteclient.GetConfigFromUrl(closerCtx, cfg.NetworkConfigUrl)
		if err != nil {
			pterm.Warning.Println("Failed to download ton config:", err.Error(), "; We will take it from static cache")
			lsCfg = &liteclient.GlobalConfig{}
			if err = json.NewDecoder(bytes.NewBufferString(config.FallbackNetworkConfig)).Decode(lsCfg); err != nil {
				pterm.Error.Println("Failed to parse fallback ton config:", err.Error())
				os.Exit(1)
			}
		}
	}

	lsClient := liteclient.NewConnectionPool()
	if err = lsClient.AddConnectionsFromConfig(closerCtx, lsCfg); err != nil {
		pterm.Error.Println("Failed to init LS client:", err.Error())
		os.Exit(1)
	}

	apiClient := ton.NewAPIClient(lsClient, ton.ProofCheckPolicyFast).WithRetry().WithTimeout(10 * time.Second)

	tunnelCtx, tunnelStop := context.WithCancel(context.Background())

	var netMgr adnl.NetManager
	var gate *adnl.Gateway
	if *EnableTunnel {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout}).Level(zerolog.InfoLevel)
		if *Verbosity >= 3 {
			log.Logger = log.Logger.Level(zerolog.DebugLevel)
		}

		if cfg.TunnelConfig.NodesPoolConfigPath == "" {
			pterm.Fatal.Println("Nodes pool config path is empty")
			return
		}

		data, err := os.ReadFile(cfg.TunnelConfig.NodesPoolConfigPath)
		if err != nil {
			pterm.Fatal.Println("Failed to load tunnel nodes pool config", err.Error())
		}

		var tunSharedCfg tunnelConfig.SharedConfig
		if err = json.Unmarshal(data, &tunSharedCfg); err != nil {
			pterm.Fatal.Println("Failed to parse tunnel shared config (nodes pool)", err.Error())
			return
		}

		events := make(chan any, 1)
		go tunnel.RunTunnel(closerCtx, cfg.TunnelConfig, &tunSharedCfg, lsCfg, log.Logger, events)

		initUpd := make(chan tunnel.UpdatedEvent, 1)
		once := sync.Once{}
		go func() {
			for event := range events {
				switch e := event.(type) {
				case tunnel.StoppedEvent:
					tunnelStop()
					return
				case tunnel.UpdatedEvent:
					log.Info().Msg("tunnel updated")

					e.Tunnel.SetOutAddressChangedHandler(func(addr *net.UDPAddr) {
						log.Info().Str("addr", addr.IP.String()).Int("port", addr.Port).Msg("out updated for storage")

						gate.SetAddressList([]*adnlAddress.UDP{
							{
								IP:   addr.IP,
								Port: int32(addr.Port),
							},
						})
					})

					once.Do(func() {
						initUpd <- e
					})
				case tunnel.ConfigurationErrorEvent:
					log.Err(e.Err).Msg("tunnel configuration error, will retry...")
				case error:
					log.Fatal().Err(e).Msg("tunnel failed")
				}
			}
		}()

		upd := <-initUpd
		netMgr = adnl.NewMultiNetReader(upd.Tunnel)

		gate = adnl.NewGatewayWithNetManager(cfg.Key, netMgr)

		pterm.Success.Println("Using tunnel:", upd.ExtIP.String())
	} else {
		dl, err := adnl.DefaultListener(cfg.ListenAddr)
		if err != nil {
			pterm.Fatal.Println(cfg.ListenAddr, err.Error())
			return
		}
		netMgr = adnl.NewMultiNetReader(dl)
		gate = adnl.NewGatewayWithNetManager(cfg.Key, netMgr)
	}

	listenThreads := runtime.NumCPU()
	if listenThreads > 40 {
		listenThreads = 40
	}
	if *ListenThreads > 0 {
		listenThreads = *ListenThreads
	}

	serverMode := ip != nil
	if ip != nil {
		gate.SetAddressList([]*adnlAddress.UDP{
			{
				IP:   ip,
				Port: int32(port),
			},
		})

		err = gate.StartServer(cfg.ListenAddr, listenThreads)
		if err != nil {
			pterm.Error.Println("Failed to start adnl gateway in server mode:", err.Error())
			os.Exit(1)
		}
	} else {
		err = gate.StartClient(listenThreads)
		if err != nil {
			pterm.Error.Println("Failed to start adnl gateway in client mode:", err.Error())
			os.Exit(1)
		}
	}

	_, dhtKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		pterm.Error.Println(err.Error())
		return
	}

	dhtGate := adnl.NewGatewayWithNetManager(dhtKey, netMgr)
	if err = dhtGate.StartClient(); err != nil {
		pterm.Error.Println("Failed to init dht adnl gateway:", err.Error())
		os.Exit(1)
	}

	dhtClient, err := dht.NewClientFromConfig(dhtGate, lsCfg)
	if err != nil {
		pterm.Error.Println("Failed to init dht client:", err.Error())
		os.Exit(1)
	}

	providerGate := adnl.NewGateway(cfg.Key)
	if err = providerGate.StartClient(); err != nil {
		pterm.Error.Println("Failed to init provider gateway:", err.Error())
		os.Exit(1)
	}

	srv := storage.NewServer(dhtClient, gate, cfg.Key, serverMode, *DHTParallelism)
	Connector = storage.NewConnector(srv)

	Storage, err = db.NewStorage(ldb, Connector, *ForcePieceSize, true, *NoVerify, *NoRemove, nil)
	if err != nil {
		pterm.Error.Println("Failed to init storage:", err.Error())
		os.Exit(1)
	}
	srv.SetStorage(Storage)

	Provider = provider.NewClient(Storage, apiClient, transport.NewClient(providerGate, dhtClient))

	pterm.Info.Println("We have telegram group, subscribe to stay updated or ask some questions.", pterm.LightBlue("https://t.me/tonrh"))

	pterm.Success.Println("Storage started, server mode:", serverMode)

	if *API != "" {
		a := api.NewServer(Connector, Storage, Config.DownloadsPath)

		if *CredentialsLogin != "" && *CredentialsPassword != "" {
			a.SetCredentials(&api.Credentials{
				Login:    *CredentialsLogin,
				Password: *CredentialsPassword,
			})
		} else if *CredentialsLogin == "" && *CredentialsPassword != "" ||
			*CredentialsLogin != "" && *CredentialsPassword == "" {
			pterm.Error.Println("Both login and password for API should be set or not set")
			os.Exit(1)
		}

		go func() {
			if err := a.Start(*API); err != nil {
				pterm.Error.Println("Failed to start API on", *API, "err:", err.Error())
				os.Exit(1)
			}
		}()
		pterm.Success.Println("Storage HTTP API on", *API)
	}

	onStop := func() {
		stop()

		pterm.Info.Println("Stopping...")
		if *EnableTunnel {
			pterm.Info.Println("Closing tunnel...")
			<-tunnelCtx.Done()
		}
		pterm.Info.Println("Stopped")
		os.Exit(0)
	}

	if !*IsDaemon {
		go func() {
			list()

			for {
				var cmd string
				var err error
				if !isatty.IsTerminal(os.Stdout.Fd()) {
					fmt.Print("Command: ")
					in := bufio.NewReader(os.Stdin)
					cmd, err = in.ReadString('\n')
				} else {
					cmd, err = pterm.DefaultInteractiveTextInput.WithOnInterruptFunc(onStop).Show("Command")
				}

				if err != nil {
					pterm.Warning.Println("unexpected input:" + err.Error())
					continue
				}

				parts := strings.Split(cmd, " ")
				if len(parts) == 0 {
					continue
				}

				switch parts[0] {
				case "download":
					if len(parts) < 2 {
						pterm.Error.Println("Usage: download [bag_id]")
						continue
					}
					download(parts[1])
				case "create":
					if len(parts) < 3 {
						pterm.Error.Println("Usage: create [path] [description]")
						continue
					}
					create(parts[1], parts[2])
				case "remove":
					if len(parts) < 3 {
						pterm.Error.Println("Usage: remove [bag_id] [with files? (true/false)]")
						continue
					}
					remove(parts[1], strings.ToLower(parts[2]) == "true")
				case "list":
					list()
				case "providers":
					if len(parts) < 3 {
						pterm.Error.Println("Usage: providers [bag_id] [owner_address]")
						continue
					}

					listProviders(closerCtx, parts[1], parts[2])
				case "rent-storage":
					if len(parts) < 5 {
						pterm.Error.Println("Usage: rent-storage [bag_id] [owner_address] [provider_id] [amount]")
						continue
					}

					rentStorage(closerCtx, parts[1], parts[2], parts[3], parts[4])
				case "rent-withdraw":
					if len(parts) < 4 {
						pterm.Error.Println("Usage: rent-withdraw [bag_id] [owner_address] [amount]")
						continue
					}

					rentWithdraw(closerCtx, parts[1], parts[2], parts[3])
				case "rent-topup":
					if len(parts) < 4 {
						pterm.Error.Println("Usage: rent-topup [bag_id] [owner_address] [amount]")
						continue
					}

					rentTopup(closerCtx, parts[1], parts[2], parts[3])
				default:
					fallthrough
				case "help":
					pterm.Info.Println("Commands:\n"+
						"create [path] [description]\n",
						"download [bag_id]\n",
						"remove [bag_id] [with files? (true/false)]\n",
						"list\n",
						"providers [bag_id] [owner_address]\n",
						"rent-storage [bag_id] [owner_address] [provider_id] [amount]\n",
						"rent-withdraw [bag_id] [owner_address] [amount]\n",
						"rent-topup [bag_id] [owner_address] [amount]\n",
						"help\n",
					)
				}
			}
		}()
	}

	<-make(chan struct{})
}

func download(bagId string) {
	bag, err := hex.DecodeString(bagId)
	if err != nil {
		pterm.Error.Println("Invalid bag id:", err.Error())
		return
	}

	if len(bag) != 32 {
		pterm.Error.Println("Invalid bag id: should be 32 bytes hex")
		return
	}

	tor := Storage.GetTorrent(bag)
	if tor == nil {
		tor = storage.NewTorrent(filepath.Join(Config.DownloadsPath, bagId), Storage, Connector)
		tor.BagID = bag

		if err = tor.Start(true, true, false); err != nil {
			pterm.Error.Println("Failed to start:", err.Error())
			return
		}

		err = Storage.SetTorrent(tor)
		if err != nil {
			pterm.Error.Println("Failed to set storage:", err.Error())
			os.Exit(1)
		}
	} else {
		if err = tor.Start(true, true, false); err != nil {
			pterm.Error.Println("Failed to start:", err.Error())
			return
		}
	}

	pterm.Success.Println("Bag added")
}

func remove(bagId string, withFiles bool) {
	bag, err := hex.DecodeString(bagId)
	if err != nil {
		pterm.Error.Println("Invalid bag id:", err.Error())
		return
	}

	if len(bag) != 32 {
		pterm.Error.Println("Invalid bag id: should be 32 bytes hex")
		return
	}

	tor := Storage.GetTorrent(bag)
	if tor == nil {
		pterm.Error.Println("Bag not found")
		return
	}

	err = Storage.RemoveTorrent(tor, withFiles)
	if err != nil {
		pterm.Error.Println("Failed to remove:", err.Error())
		return
	}
	pterm.Success.Println("Bag removed")
}

func create(path, name string) {
	rootPath, dirName, files, err := Storage.DetectFileRefs(path)
	if err != nil {
		pterm.Error.Println("Failed to read file refs:", err.Error())
		return
	}

	it, err := storage.CreateTorrent(context.Background(), rootPath, dirName, name, Storage, Connector, files, nil)
	if err != nil {
		pterm.Error.Println("Failed to create bag:", err.Error())
		return
	}
	it.Start(true, true, false)

	err = Storage.SetTorrent(it)
	if err != nil {
		pterm.Error.Println("Failed to add bag:", err.Error())
		return
	}

	pterm.Success.Println("Bag created and ready:", pterm.Cyan(hex.EncodeToString(it.BagID)))
	list()
}

func list() {
	var table = pterm.TableData{
		{"Bag ID", "Description", "Downloaded", "Size", "Peers", "Download", "Upload", "Status", "Uploaded"},
	}

	var totalDow, totalUpl uint64
	for _, t := range Storage.GetAll() {
		var strDownloaded, uploaded, strFull, description = "0 Bytes", "0 Bytes", "???", "???"
		status := "Resolving"

		activeDownload, activeUpload := t.IsActive()
		if !activeDownload {
			status = "Inactive"
		}

		verifyInProgress, _ := t.GetLastVerifiedAt()

		if t.Info != nil {
			mask := t.PiecesMask()
			downloadedPieces := 0
			for _, b := range mask {
				downloadedPieces += bits.OnesCount8(b)
			}
			full := t.Info.FileSize - t.Info.HeaderSize
			downloaded := uint64(downloadedPieces*int(t.Info.PieceSize)) - t.Info.HeaderSize
			if uint64(downloadedPieces*int(t.Info.PieceSize)) < t.Info.HeaderSize { // 0 if header not fully downloaded
				downloaded = 0
			}
			if downloaded > full { // cut not full last piece
				downloaded = full
			}

			if !verifyInProgress {
				if downloaded == full {
					status = "Downloaded"
					if activeUpload {
						status = "Seeding"
					}
				} else if activeDownload {
					if len(t.GetActiveFilesIDs()) == 0 && !t.IsDownloadAll() {
						status = "Header downloaded"
					} else {
						status = "Downloading"
					}
				}
			} else {
				status = "Verifying"
			}

			strDownloaded = storage.ToSz(downloaded)
			strFull = storage.ToSz(full)
			description = t.Info.Description.Value

			uploaded = storage.ToSz(t.GetUploadStats())
		}

		var dow, upl, num uint64
		for _, p := range t.GetPeers() {
			dow += p.GetDownloadSpeed()
			upl += p.GetUploadSpeed()
			num++
		}
		totalDow += dow
		totalUpl += upl

		table = append(table, []string{hex.EncodeToString(t.BagID), description,
			strDownloaded, strFull, fmt.Sprint(num),
			storage.ToSpeed(dow), storage.ToSpeed(upl), status, uploaded})
	}

	if len(table) > 1 {
		pterm.Println("Active bags (" + pterm.Cyan(fmt.Sprint(len(table)-1)) + ")")
		pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(table).Render()
		pterm.Println("Total speed: D " + pterm.Cyan(fmt.Sprint(storage.ToSpeed(totalDow))) + " U " + pterm.Cyan(fmt.Sprint(storage.ToSpeed(totalUpl))))
	}
}

func listProviders(ctx context.Context, bagId, strAddr string) {
	bag, err := hex.DecodeString(bagId)
	if err != nil {
		pterm.Error.Println("Invalid bag id:", err.Error())
		return
	}

	if len(bag) != 32 {
		pterm.Error.Println("Invalid bag id: should be 32 bytes hex")
		return
	}

	addr, err := address.ParseAddr(strAddr)
	if err != nil {
		pterm.Warning.Println("Invalid address format")
		return
	}

	tor := Storage.GetTorrent(bag)
	if tor == nil {
		pterm.Error.Println("Bag not found")
		return
	}

	data, err := Provider.FetchProviderContract(ctx, bag, addr)
	if err != nil {
		pterm.Error.Println("Failed to fetch contract data: " + err.Error())
		return
	}

	var table = pterm.TableData{
		{"Provider ID", "Status", "Price", "Is Peer", "Last Proof At", "Proof Every"},
	}

	peers := tor.GetPeers()

	for _, p := range data.Providers {
		every := ""
		if p.MaxSpan < 3600 {
			every = fmt.Sprint(p.MaxSpan/60) + " Minutes"
		} else if p.MaxSpan < 100*3600 {
			every = fmt.Sprint(p.MaxSpan/3600) + " Hours"
		} else {
			every = fmt.Sprint(p.MaxSpan/86400) + " Days"
		}

		since := "Never"
		snc := time.Since(p.LastProofAt)
		if snc < 2*time.Minute {
			since = fmt.Sprint(int(snc.Seconds())) + " Seconds ago"
		} else if snc < 2*time.Hour {
			since = fmt.Sprint(int(snc.Minutes())) + " Minutes ago"
		} else if snc < 48*time.Hour {
			since = fmt.Sprint(int(snc.Hours())) + " Hours ago"
		} else if snc < 1000*24*time.Hour {
			since = fmt.Sprint(int(snc.Hours())/24) + " Days ago"
		}

		ratePerMB := new(big.Float).SetInt(p.RatePerMB.Nano())
		szMB := new(big.Float).Quo(new(big.Float).SetUint64(data.Size), big.NewFloat(1024*1024))
		perDay := new(big.Float).Mul(ratePerMB, szMB)

		perDayNano, _ := perDay.Int(nil)

		info, err := Provider.RequestProviderStorageInfo(ctx, bag, p.Key, addr)
		if err != nil {
			table = append(table, []string{strings.ToUpper(hex.EncodeToString(p.Key)), "Failed",
				tlb.FromNanoTON(perDayNano).String(),
				fmt.Sprint(false), since, every})
			continue
		}

		isPeer := peers[strings.ToLower(info.StorageADNL)].Addr != ""

		status := info.Status

		if status == "active" {
			since = "Just now"
		} else if status == "error" {
			status = "Error (" + info.Reason + ")"
		}

		if len(status) > 1 {
			status = strings.ToUpper(status[:1]) + status[1:]
		}

		table = append(table, []string{strings.ToUpper(hex.EncodeToString(p.Key)), status,
			tlb.FromNanoTON(perDayNano).String(),
			fmt.Sprint(isPeer), since, every})
	}

	if len(table) > 1 {
		pterm.Printfln("Balance is: %s, Contract address: %s", pterm.Cyan(data.Balance.String()), data.Address.String())
		pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(table).Render()
	} else {
		pterm.Println("No providers")
	}
}

func rentWithdraw(ctx context.Context, bagId, strAddr, amount string) {
	bag, err := hex.DecodeString(bagId)
	if err != nil {
		pterm.Error.Println("Invalid bag id:", err.Error())
		return
	}

	if len(bag) != 32 {
		pterm.Error.Println("Invalid bag id: should be 32 bytes hex")
		return
	}

	addr, err := address.ParseAddr(strAddr)
	if err != nil {
		pterm.Warning.Println("Invalid address format")
		return
	}

	amt, err := tlb.FromTON(amount)
	if err != nil {
		pterm.Error.Println("Incorrect amount format: " + err.Error())
		return
	}

	tor := Storage.GetTorrent(bag)
	if tor == nil {
		pterm.Error.Println("Bag not found")
		return
	}

	data, err := Provider.FetchProviderContract(ctx, bag, addr)
	if err != nil {
		pterm.Error.Println("Failed to fetch contract data: " + err.Error())
		return
	}

	contractAddr, body, err := Provider.BuildWithdrawalTransaction(bag, addr)
	if err != nil {
		pterm.Error.Println("Failed to fetch contract data: " + err.Error())
		return
	}

	tx := "ton://transfer/" + contractAddr.String() + "?bin=" + base64.URLEncoding.EncodeToString(body) + "&amount=" + amt.Nano().String()

	pterm.Printfln("Balance is: %s, Contract address: %s", pterm.Cyan(data.Balance.String()), data.Address.String())
	pterm.Info.Println("To withdraw balance execute this transaction:\n" + pterm.Magenta(tx))
}

func rentTopup(ctx context.Context, bagId, strAddr, amount string) {
	bag, err := hex.DecodeString(bagId)
	if err != nil {
		pterm.Error.Println("Invalid bag id:", err.Error())
		return
	}

	if len(bag) != 32 {
		pterm.Error.Println("Invalid bag id: should be 32 bytes hex")
		return
	}

	addr, err := address.ParseAddr(strAddr)
	if err != nil {
		pterm.Warning.Println("Invalid address format")
		return
	}

	amt, err := tlb.FromTON(amount)
	if err != nil {
		pterm.Error.Println("Incorrect amount format: " + err.Error())
		return
	}

	tor := Storage.GetTorrent(bag)
	if tor == nil {
		pterm.Error.Println("Bag not found")
		return
	}

	data, err := Provider.FetchProviderContract(ctx, bag, addr)
	if err != nil {
		pterm.Error.Println("Failed to fetch contract data: " + err.Error())
		return
	}

	tx := "ton://transfer/" + data.Address.String() + "?amount=" + amt.Nano().String()

	pterm.Printfln("Balance is: %s, Contract address: %s", pterm.Cyan(data.Balance.String()), data.Address.String())
	pterm.Info.Println("To withdraw balance execute this transaction:\n" + pterm.Magenta(tx))
}

func rentStorage(ctx context.Context, bagId, addrStr, providerId, amount string) {
	bag, err := hex.DecodeString(bagId)
	if err != nil {
		pterm.Error.Println("Invalid bag id:", err.Error())
		return
	}

	if len(bag) != 32 {
		pterm.Error.Println("Invalid bag id: should be 32 bytes hex")
		return
	}

	prv, err := hex.DecodeString(providerId)
	if err != nil {
		pterm.Error.Println("Invalid provider id:", err.Error())
		return
	}

	if len(prv) != 32 {
		pterm.Error.Println("Invalid provider id: should be 32 bytes hex")
		return
	}

	amt, err := tlb.FromTON(amount)
	if err != nil {
		pterm.Error.Println("Incorrect amount format: " + err.Error())
		return
	}

	tor := Storage.GetTorrent(bag)
	if tor == nil {
		pterm.Error.Println("Bag is not exists")
		return
	}

	_, activeUpl := tor.IsActive()
	if !activeUpl || tor.Header == nil {
		pterm.Error.Println("Bag is not active for upload")
		return
	}

	rates, err := Provider.FetchProviderRates(ctx, bag, prv)
	if err != nil {
		pterm.Error.Println("Failed to fetch rates:", err.Error())
		return
	}

	if rates.SpaceAvailableMB < rates.Size {
		pterm.Warning.Println("Torrent is too big for this provider", err.Error())
		return
	}

	if !rates.Available {
		pterm.Warning.Println("Provider is currently not accepting storage requests", err.Error())
		return
	}

	offer := provider.CalculateBestProviderOffer(rates)

	pterm.Success.Println("Storage rate for hosting this bag of provider is: " + pterm.Cyan(tlb.FromNanoTON(offer.PerDayNano).String()+" TON") + " per day." +
		"\nProvider will proof to contract every " + pterm.Cyan(offer.Every) +
		"\nIf you agree, please type " + pterm.LightGreen("YES"))

	cmd, err := pterm.DefaultInteractiveTextInput.Show("You agree?")
	if err != nil {
		pterm.Warning.Println("unexpected input")
		return
	}

	if strings.ToLower(cmd) != "yes" {
		pterm.Info.Println("Storage proposal was declined")
		return
	}

	addr, err := address.ParseAddr(addrStr)
	if err != nil {
		pterm.Warning.Println("Invalid address format")
		return
	}

	providers := []provider.NewProviderData{
		{
			Address:       address.NewAddress(0, 0, prv),
			MaxSpan:       offer.Span,
			PricePerMBDay: tlb.FromNanoTON(offer.RatePerMBNano),
		},
	}

	contractData, err := Provider.FetchProviderContract(context.Background(), bag, addr)
	if err != nil {
		if !errors.Is(err, contract.ErrNotDeployed) {
			pterm.Error.Println("Failed to calculate contract info: " + err.Error())
			return
		}
	} else {
	skip:
		for _, p := range contractData.Providers {
			for _, pe := range providers {
				if bytes.Equal(pe.Address.Data(), p.Key) {
					continue skip
				}
			}

			providers = append(providers, provider.NewProviderData{
				Address:       address.NewAddress(0, 0, p.Key),
				MaxSpan:       p.MaxSpan,
				PricePerMBDay: p.RatePerMB,
			})
		}
	}

	contractAddr, body, stateInit, err := Provider.BuildAddProviderTransaction(context.Background(), bag, addr, providers)
	if err != nil {
		pterm.Error.Println("Failed to build transaction: " + err.Error())
		return
	}

	tx := "ton://transfer/" + contractAddr.String() + "?bin=" + base64.URLEncoding.EncodeToString(body) + "&init=" + base64.URLEncoding.EncodeToString(stateInit) + "&amount=" + amt.Nano().String()
	pterm.Info.Println("Use this url to execute transaction:\n" + pterm.Magenta(tx))
}
