package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	zl "github.com/rs/zerolog"
	zlg "github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/ton-blockchain/adnl-tunnel/tunnel"
	"github.com/xssnick/ton-payment-network/pkg/payments"
	"github.com/xssnick/ton-payment-network/tonpayments"
	"github.com/xssnick/ton-payment-network/tonpayments/chain"
	configPayments "github.com/xssnick/ton-payment-network/tonpayments/config"
	dbPayments "github.com/xssnick/ton-payment-network/tonpayments/db"
	leveldbPayments "github.com/xssnick/ton-payment-network/tonpayments/db/leveldb"
	transportPayments "github.com/xssnick/ton-payment-network/tonpayments/transport"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"github.com/xssnick/tonutils-storage-provider/pkg/contract"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
	"github.com/xssnick/tonutils-storage/api"
	"github.com/xssnick/tonutils-storage/config"
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/provider"
	"github.com/xssnick/tonutils-storage/storage"
	"log"
	"math/big"
	"math/bits"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

import _ "net/http/pprof"

var (
	API                 = flag.String("api", "", "HTTP API listen address")
	CredentialsLogin    = flag.String("api-login", "", "HTTP API credentials login")
	CredentialsPassword = flag.String("api-password", "", "HTTP API credentials password")
	DBPath              = flag.String("db", "tonutils-storage-db", "Path to db folder")
	Verbosity           = flag.Int("verbosity", 2, "Debug logs")
	IsDaemon            = flag.Bool("daemon", false, "Daemon mode, no command line input")
	NetworkConfigPath   = flag.String("network-config", "", "Network config path to load from disk")
	Version             = flag.Bool("version", false, "Show version and exit")
	ListenThreads       = flag.Int("threads", 0, "Listen threads")
)

var GitCommit string

var Storage *db.Storage
var Provider *provider.Client
var Connector storage.NetConnector

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
		adnl.Logger = log.Println
		fallthrough
	case 12:
		dht.Logger = log.Println
		fallthrough
	case 11:
		storage.Logger = log.Println
		provider.Logger = log.Println
	}

	_ = pterm.DefaultBigText.WithLetters(
		putils.LettersFromStringWithStyle("Ton", pterm.FgBlue.ToStyle()),
		putils.LettersFromStringWithStyle("Utils", pterm.FgLightBlue.ToStyle())).
		Render()

	pterm.DefaultBox.WithBoxStyle(pterm.NewStyle(pterm.FgLightBlue)).Println(pterm.LightWhite("   Storage   "))
	pterm.Info.Println("Version:", GitCommit)

	if *DBPath == "" {
		pterm.Error.Println("DB path should be specified with -db flag")
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*DBPath)
	if err != nil {
		pterm.Error.Println("Failed to load config:", err.Error())
		os.Exit(1)
	}

	ldb, err := leveldb.OpenFile(*DBPath+"/db", nil)
	if err != nil {
		pterm.Error.Println("Failed to load db:", err.Error())
		os.Exit(1)
	}

	var ip net.IP
	if cfg.ExternalIP != "" {
		ip = net.ParseIP(cfg.ExternalIP)
		if ip == nil {
			pterm.Error.Println("External ip is invalid")
			os.Exit(1)
		}
	}

	var lsCfg *liteclient.GlobalConfig
	if *NetworkConfigPath != "" {
		lsCfg, err = liteclient.GetConfigFromFile(*NetworkConfigPath)
		if err != nil {
			pterm.Error.Println("Failed to load ton network config from file:", err.Error())
			os.Exit(1)
		}
	} else {
		lsCfg, err = liteclient.GetConfigFromUrl(context.Background(), cfg.NetworkConfigUrl)
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
	if err = lsClient.AddConnectionsFromConfig(context.Background(), lsCfg); err != nil {
		pterm.Error.Println("Failed to init LS client:", err.Error())
		os.Exit(1)
	}

	apiClient := ton.NewAPIClient(lsClient, ton.ProofCheckPolicyFast).WithRetry().WithTimeout(10 * time.Second)

	runtime.SetBlockProfileRate(1)
	go func() {
		log.Println(http.ListenAndServe(":6063", nil))
	}()

	var gate *adnl.Gateway
	if cfg.Tunnel.Enabled {
		var ipTun net.IP
		var portTun uint16
		var tun *tunnel.RegularOutTunnel
		tun, portTun, ipTun, err = prepareTun(cfg, apiClient)
		if err != nil {
			pterm.Fatal.Println(err.Error())
			return
		}

		gate = adnl.NewGatewayWithListener(cfg.Key, func(addr string) (net.PacketConn, error) {
			return tun, nil
		})

		if ipTun != nil {
			pterm.Success.Println("Using tunnel:", ipTun.String())
			if ip != nil {
				ip = ipTun
				_ = portTun
				panic("server mode not yet ready")
				// gate.SetExternalPort(portTun)
			}
		} else {
			pterm.Warning.Println("Using tunnel: ???")
		}
	} else {
		gate = adnl.NewGateway(cfg.Key)
	}

	listenThreads := runtime.NumCPU()
	if listenThreads > 32 {
		listenThreads = 32
	}
	if *ListenThreads > 0 {
		listenThreads = *ListenThreads
	}

	serverMode := ip != nil
	if serverMode {
		gate.SetExternalIP(ip)
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

	dhtGate := adnl.NewGateway(cfg.Key)
	if err = dhtGate.StartClient(); err != nil {
		pterm.Error.Println("Failed to init dht adnl gateway:", err.Error())
		os.Exit(1)
	}

	dhtClient, err := dht.NewClientFromConfig(dhtGate, lsCfg)
	if err != nil {
		pterm.Error.Println("Failed to init dht client:", err.Error())
		os.Exit(1)
	}

	downloadGate := adnl.NewGateway(cfg.Key)
	if err = downloadGate.StartClient(); err != nil {
		pterm.Error.Println("Failed to init downloader gateway:", err.Error())
		os.Exit(1)
	}

	providerGate := adnl.NewGateway(cfg.Key)
	if err = providerGate.StartClient(); err != nil {
		pterm.Error.Println("Failed to init provider gateway:", err.Error())
		os.Exit(1)
	}

	srv := storage.NewServer(dhtClient, gate, cfg.Key, serverMode)
	Connector = storage.NewConnector(srv)

	Storage, err = db.NewStorage(ldb, Connector, true, nil)
	if err != nil {
		pterm.Error.Println("Failed to init storage:", err.Error())
		os.Exit(1)
	}
	srv.SetStorage(Storage)

	Provider = provider.NewClient(Storage, apiClient, transport.NewClient(providerGate, dhtClient))

	pterm.Info.Println("If you use it for commercial purposes please consider", pterm.LightWhite("donation")+". It allows us to develop such products 100% free.")
	pterm.Info.Println("We also have telegram group, subscribe to stay updated or ask some questions.", pterm.LightBlue("https://t.me/tonrh"))

	pterm.Success.Println("Storage started, server mode:", serverMode)

	if *API != "" {
		a := api.NewServer(Connector, Storage)

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

	if !*IsDaemon {
		go func() {
			list()

			for {
				cmd, err := pterm.DefaultInteractiveTextInput.Show("Command")
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

					listProviders(context.Background(), parts[1], parts[2])
				case "rent-storage":
					if len(parts) < 5 {
						pterm.Error.Println("Usage: rent-storage [bag_id] [owner_address] [provider_id] [amount]")
						continue
					}

					rentStorage(context.Background(), parts[1], parts[2], parts[3], parts[4])
				case "rent-withdraw":
					if len(parts) < 4 {
						pterm.Error.Println("Usage: rent-withdraw [bag_id] [owner_address] [amount]")
						continue
					}

					rentWithdraw(context.Background(), parts[1], parts[2], parts[3])
				case "rent-topup":
					if len(parts) < 4 {
						pterm.Error.Println("Usage: rent-topup [bag_id] [owner_address] [amount]")
						continue
					}

					rentTopup(context.Background(), parts[1], parts[2], parts[3])
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

	sig := make(chan os.Signal, 1)
	signal.Notify(sig,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sig
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
		tor = storage.NewTorrent(*DBPath+"/downloads/"+bagId, Storage, Connector)
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

	for _, t := range Storage.GetAll() {
		var strDownloaded, uploaded, strFull, description = "0 Bytes", "0 Bytes", "???", "???"
		status := "Resolving"

		activeDownload, activeUpload := t.IsActive()
		if !activeDownload {
			status = "Inactive"
		}

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
			if downloaded == full {
				status = "Downloaded"
				if activeUpload {
					status = "Seeding"
				}
			} else if activeDownload {
				status = "Downloading"
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

		table = append(table, []string{hex.EncodeToString(t.BagID), description,
			strDownloaded, strFull, fmt.Sprint(num),
			storage.ToSpeed(dow), storage.ToSpeed(upl), status, uploaded})
	}

	if len(table) > 1 {
		pterm.Println("Active bags")
		pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(table).Render()
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

		isPeer := !peers[strings.ToLower(info.StorageADNL)].LastSeenAt.IsZero()

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

	span := uint32(86400)
	if span > rates.MaxSpan {
		span = rates.MaxSpan
	} else if span < rates.MinSpan {
		span = rates.MinSpan
	}

	every := ""
	if span < 3600 {
		every = fmt.Sprint(span/60) + " minutes"
	} else if span < 100*3600 {
		every = fmt.Sprint(span/3600) + " hours"
	} else {
		every = fmt.Sprint(span/86400) + " days"
	}

	ratePerMB := new(big.Float).SetInt(rates.RatePerMBDay.Nano())
	min := new(big.Float).SetInt(rates.MinBounty.Nano())

	szMB := new(big.Float).Quo(new(big.Float).SetUint64(rates.Size), big.NewFloat(1024*1024))
	perDay := new(big.Float).Mul(ratePerMB, szMB)
	if perDay.Cmp(min) < 0 {
		// increase reward to fit min bounty
		coff := new(big.Float).Quo(min, perDay)
		coff = coff.Add(coff, big.NewFloat(0.01)) // increase a bit to not be less than needed

		ratePerMB = new(big.Float).Mul(ratePerMB, coff)
		perDay = new(big.Float).Mul(ratePerMB, szMB)
	}

	ratePerMBNano, _ := ratePerMB.Int(nil)
	perDayNano, _ := perDay.Int(nil)

	pterm.Success.Println("Storage rate for hosting this bag of provider is: " + pterm.Cyan(tlb.FromNanoTON(perDayNano).String()+" TON") + " per day." +
		"\nProvider will proof to contract every " + pterm.Cyan(every) +
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
			MaxSpan:       span,
			PricePerMBDay: tlb.FromNanoTON(ratePerMBNano),
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

func prepareTun(cfg *config.Config, apiClient ton.APIClientWrapped) (*tunnel.RegularOutTunnel, uint16, net.IP, error) {
	_, dhtKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to generate DHT key: %w", err)
	}

	_, tunKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to generate TUN key: %w", err)
	}

	gate := adnl.NewGateway(tunKey)
	if err = gate.StartClient(8); err != nil {
		return nil, 0, nil, fmt.Errorf("start gateway as client failed: %w", err)
	}

	dhtGate := adnl.NewGateway(dhtKey)
	if err = dhtGate.StartClient(); err != nil {
		return nil, 0, nil, fmt.Errorf("start dht gateway failed: %w", err)
	}

	dhtClient, err := dht.NewClientFromConfigUrl(context.Background(), dhtGate, "https://ton-blockchain.github.io/global.config.json")
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to create DHT client: %w", err)
	}

	/*
		nodes, err := tGate.DiscoverNodes(context.Background())
		if err != nil {
			return nil, nil, fmt.Errorf("discover nodes failed: %w", err)
		}

		if len(nodes) == 0 {
			return nil, nil, fmt.Errorf("no nodes found")
		}

		nodes = nodes[:1]*/

	var chainTo, chainFrom []*tunnel.SectionInfo

	for i, s := range cfg.Tunnel.RouteOut {
		si, err := paymentConfigToSections(&s, cfg.Tunnel.Payments.Enabled)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("convert config to section %d in `out` route failed: %w", i, err)
		}

		chainTo = append(chainTo, si)
	}

	for i, s := range cfg.Tunnel.RouteIn {
		si, err := paymentConfigToSections(&s, cfg.Tunnel.Payments.Enabled)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("convert config to section %d in `in` route failed: %w", i, err)
		}

		chainFrom = append(chainFrom, si)
	}

	siGate, err := paymentConfigToSections(&cfg.Tunnel.OutGateway, cfg.Tunnel.Payments.Enabled)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("convert config to section out gateway failed: %w", err)
	}

	chainTo = append(chainTo, siGate)

	toUs, err := tunnel.GenerateEncryptionKeys(tunKey.Public().(ed25519.PublicKey))
	if err != nil {
		return nil, 0, nil, fmt.Errorf("generate us encryption keys failed: %w", err)
	}
	chainFrom = append(chainFrom, &tunnel.SectionInfo{
		Keys: toUs,
	})

	zLogger := zl.New(zl.NewConsoleWriter()).With().Timestamp().Logger().Level(zl.InfoLevel)
	if *Verbosity >= 3 {
		zLogger = zLogger.Level(zl.DebugLevel).With().Logger()
	} else if *Verbosity == 2 {
		zLogger = zLogger.Level(zl.InfoLevel).With().Logger()
	} else if *Verbosity == 1 {
		zLogger = zLogger.Level(zl.WarnLevel).With().Logger()
	} else if *Verbosity == 0 {
		zLogger = zLogger.Level(zl.ErrorLevel).With().Logger()
	} else {
		zLogger = zLogger.Level(zl.FatalLevel).With().Logger()
	}
	zlg.Logger = zLogger

	scanLog := zl.Nop()
	if *Verbosity >= 4 {
		scanLog = zLogger.Level(zl.DebugLevel).With().Str("component", "payments").Logger()
	}

	var pay *tonpayments.Service
	if cfg.Tunnel.Payments.Enabled {
		pay, err = preparePayments(context.Background(), apiClient, dhtClient, cfg, scanLog)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("prepare payments failed: %w", err)
		}
	}

	tGate := tunnel.NewGateway(gate, dhtClient, tunKey, zLogger.With().Str("component", "gateway").Logger(), tunnel.PaymentConfig{
		Service: pay,
	})
	go func() {
		if err = tGate.Start(); err != nil {
			pterm.Fatal.Println("tunnel gateway failed", err)
			return
		}
	}()

	zLogger.Info().Msg("creating adnl tunnel...")

	tun, err := tGate.CreateRegularOutTunnel(context.Background(), chainTo, chainFrom, zLogger.With().Str("component", "tunnel").Logger())
	if err != nil {
		return nil, 0, nil, fmt.Errorf("create regular out tunnel failed: %w", err)
	}

	extIP, extPort, err := tun.WaitForInit(context.Background())
	if err != nil {
		return nil, 0, nil, fmt.Errorf("wait for tunnel init failed: %w", err)
	}

	zLogger.Info().Msg("adnl tunnel is ready")

	return tun, extPort, extIP, nil
}

func preparePayments(ctx context.Context, apiClient ton.APIClientWrapped, dhtClient *dht.Client, cfg *config.Config, logger zl.Logger) (*tonpayments.Service, error) {
	nodePrv := ed25519.NewKeyFromSeed(cfg.Tunnel.Payments.PaymentsServerKey)
	gate := adnl.NewGateway(nodePrv)

	if cfg.ExternalIP != "" {
		ip := net.ParseIP(cfg.ExternalIP)
		if ip == nil {
			return nil, fmt.Errorf("incorrect ip format")
		}

		gate.SetExternalIP(ip.To4())
		if err := gate.StartServer(cfg.Tunnel.Payments.PaymentsListenAddr); err != nil {
			return nil, fmt.Errorf("failed to init adnl gateway: %w", err)
		}
	} else {
		if err := gate.StartClient(); err != nil {
			return nil, fmt.Errorf("failed to init adnl gateway: %w", err)
		}
	}

	walletPrv := ed25519.NewKeyFromSeed(cfg.Tunnel.Payments.WalletPrivateKey)
	fdb, err := leveldbPayments.NewDB(cfg.Tunnel.Payments.DBPath, walletPrv.Public().(ed25519.PublicKey))
	if err != nil {
		return nil, fmt.Errorf("failed to init leveldb: %w", err)
	}

	tr := transportPayments.NewServer(dhtClient, gate, nodePrv, walletPrv, cfg.ExternalIP != "")

	var seqno uint32
	if bo, err := fdb.GetBlockOffset(ctx); err != nil {
		if !errors.Is(err, dbPayments.ErrNotFound) {
			return nil, fmt.Errorf("failed to load block offset: %w", err)
		}
	} else {
		seqno = bo.Seqno
	}

	inv := make(chan any)
	sc := chain.NewScanner(apiClient, payments.AsyncPaymentChannelCodeHash, seqno, logger)
	if err = sc.Start(context.Background(), inv); err != nil {
		return nil, fmt.Errorf("failed to start chain scanner: %w", err)
	}

	w, err := wallet.FromPrivateKey(apiClient, walletPrv, wallet.ConfigHighloadV3{
		MessageTTL: 3*60 + 30,
		MessageBuilder: func(ctx context.Context, subWalletId uint32) (id uint32, createdAt int64, err error) {
			createdAt = time.Now().Unix() - 30 // something older than last master block, to pass through LS external's time validation
			id = uint32(createdAt) % (1 << 23) // TODO: store seqno in db
			return
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init wallet: %w", err)
	}
	pterm.Info.Println("wallet initialized with address:", w.WalletAddress().String())

	svc := tonpayments.NewService(apiClient, fdb, tr, w, inv, walletPrv, configPayments.ChannelConfig(cfg.Tunnel.Payments.ChannelConfig))
	tr.SetService(svc)
	pterm.Success.Println("payment node initialized with public key:", base64.StdEncoding.EncodeToString(walletPrv.Public().(ed25519.PublicKey)))

	go svc.Start()
	if _, err = preparePaymentChannel(ctx, svc, nil); err != nil {
		return nil, fmt.Errorf("failed to prepare payment channel: %w", err)
	}

	return svc, nil
}

func preparePaymentChannel(ctx context.Context, pmt *tonpayments.Service, ch []byte) ([]byte, error) {
	list, err := pmt.ListChannels(ctx, nil, dbPayments.ChannelStateActive)
	if err != nil {
		return nil, fmt.Errorf("failed to list channels: %w", err)
	}

	var best []byte
	var bestAmount = big.NewInt(0)
	for _, channel := range list {
		if len(ch) > 0 {
			if bytes.Equal(channel.TheirOnchain.Key, ch) {
				// we have specified channel already deployed
				return channel.TheirOnchain.Key, nil
			}
			continue
		}

		balance, err := channel.CalcBalance(false)
		if err != nil {
			continue
		}

		if balance.Cmp(tlb.MustFromTON("0.1").Nano()) < 0 {
			// skip if balance too low
			continue
		}

		// if specific channel not defined we select the channel with the biggest deposit
		if balance.Cmp(bestAmount) >= 0 {
			bestAmount = balance
			best = channel.TheirOnchain.Key
		}
	}

	if best != nil {
		return best, nil
	}

	var inp string

	// if no channels (or specified channel) are nod deployed, we deploy
	if len(ch) == 0 {
		pterm.Warning.Println("No active onchain payment channel found, please input payment node id (pub key) in hex format, to deploy channel with:")
		if _, err = fmt.Scanln(&inp); err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		ch, err = hex.DecodeString(inp)
		if err != nil {
			return nil, fmt.Errorf("invalid id formet: %w", err)
		}
	}

	if len(ch) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid channel id length")
	}

	pterm.Warning.Println("Please input amount in TON to reserve in channel:")
	if _, err = fmt.Scanln(&inp); err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	amt, err := tlb.FromTON(inp)
	if err != nil {
		return nil, fmt.Errorf("incorrect format of amount: %w", err)
	}

	ctxTm, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	addr, err := pmt.DeployChannelWithNode(ctxTm, amt, ch, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to deploy channel with node: %w", err)
	}
	pterm.Info.Printf("Onchain channel deployed at address: %s\n", addr.String())

	return ch, nil
}

func paymentConfigToSections(s *config.TunnelRouteSection, paymentsEnabled bool) (*tunnel.SectionInfo, error) {
	if len(s.Key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid `in` route node key size")
	}

	k, err := tunnel.GenerateEncryptionKeys(s.Key)
	if err != nil {
		return nil, fmt.Errorf("generate to encryption keys failed: %w", err)
	}

	var payer *tunnel.Payer
	if s.Payment != nil {
		if !paymentsEnabled {
			return nil, fmt.Errorf("node payment is enabled but payments are disabled in config")
		}

		var ptn []tunnel.PaymentTunnelSection

		for _, paymentChain := range s.Payment.Chain {
			if len(paymentChain.NodeKey) != ed25519.PublicKeySize {
				return nil, fmt.Errorf("invalid payment node key size")
			}

			cFee, err := tlb.FromTON(paymentChain.Fee)
			if err != nil {
				return nil, fmt.Errorf("invalid payment fee: %w", err)
			}

			cCap, err := tlb.FromTON(paymentChain.MaxCapacity)
			if err != nil {
				return nil, fmt.Errorf("invalid payment capacity: %w", err)
			}

			ptn = append(ptn, tunnel.PaymentTunnelSection{
				Key:         paymentChain.NodeKey,
				Fee:         cFee.Nano(),
				MaxCapacity: cCap.Nano(),
			})
		}

		payer = &tunnel.Payer{
			PaymentTunnel:  ptn,
			PricePerPacket: s.Payment.PricePerPacketNano,
		}
	}

	return &tunnel.SectionInfo{
		Keys:        k,
		PaymentInfo: payer,
	}, nil
}
