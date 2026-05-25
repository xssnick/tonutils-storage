package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	"github.com/xssnick/tonutils-storage/internal/termui"
	"github.com/xssnick/tonutils-storage/provider"
	"github.com/xssnick/tonutils-storage/storage"
	"math/big"
	"math/bits"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	DHTParallelism      = flag.Int("dht-parallelism", 20, "Max parallel threads to search/update dht records of bags")
	PprofEnableAddr     = flag.String("pprof-addr", "", "Enable pprof HTTP server for performance profiling on specified addr")
	LimitDownload       = flag.Int("limit-download", 0, "Max bytes per second to download")
	LimitUpload         = flag.Int("limit-upload", 0, "Max bytes per second to upload")
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

	termui.ConfigurePTerm()
	termui.ConfigureStdLogger()
	level := zerolog.InfoLevel
	if *Verbosity >= 3 {
		level = zerolog.DebugLevel
	}
	termui.SetZerologLevel(level)

	storage.Logger = func(v ...any) {}
	rldp.Logger = func(v ...any) {}
	dht.Logger = func(v ...any) {}
	provider.Logger = func(...any) {}

	if *Verbosity > 13 {
		*Verbosity = 13
	}

	switch *Verbosity {
	case 13:
		adnl.Logger = func(v ...any) { log.Logger.Println(v...) }
		dht.Logger = func(v ...any) { log.Logger.Println(v...) }
		fallthrough
	case 12:
		rldp.Logger = func(v ...any) { log.Logger.Println(v...) }
		rldp.BBRLogger = func(v ...any) { log.Logger.Println(v...) }
		fallthrough
	case 11:
		storage.Logger = func(v ...any) { log.Logger.Println(v...) }
		provider.Logger = func(v ...any) { log.Logger.Println(v...) }
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

						gate.SetAddressList([]adnlAddress.Address{
							&adnlAddress.UDP{
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
	if listenThreads > 80 {
		listenThreads = 80
	}
	if *ListenThreads > 0 {
		listenThreads = *ListenThreads
	}

	serverMode := ip != nil
	if ip != nil {
		gate.SetAddressList([]adnlAddress.Address{
			&adnlAddress.UDP{
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
	if *LimitDownload > 0 {
		Connector.SetDownloadLimit(uint64(*LimitDownload))
	}
	if *LimitUpload > 0 {
		Connector.SetUploadLimit(uint64(*LimitUpload))
	}

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
				cmd, err := pterm.DefaultInteractiveTextInput.WithOnInterruptFunc(onStop).Show("Command")
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
				case "verify":
					bagID, showFiles := parseVerifyArgs(parts)
					if bagID == "" {
						pterm.Error.Println("Usage: verify [bag_id] [files]")
						continue
					}
					verify(bagID, showFiles)
				case "verify_all":
					workers, err := parseVerifyAllWorkers(parts)
					if err != nil {
						pterm.Error.Println("Usage: verify_all [workers]")
						continue
					}
					verifyAll(workers)
				case "info":
					if len(parts) < 2 {
						pterm.Error.Println("Usage: info [bag_id]")
						continue
					}
					info(parts[1])
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
					pterm.Info.Print("Commands:\n" +
						"create [path] [description]\n" +
						"download [bag_id]\n" +
						"info [bag_id]\n" +
						"verify [bag_id] [files]\n" +
						"verify_all [workers]\n" +
						"remove [bag_id] [with files? (true/false)]\n" +
						"list\n" +
						"providers [bag_id] [owner_address]\n" +
						"rent-storage [bag_id] [owner_address] [provider_id] [amount]\n" +
						"rent-withdraw [bag_id] [owner_address] [amount]\n" +
						"rent-topup [bag_id] [owner_address] [amount]\n" +
						"help\n")
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

const verifyPiecesLineWidth = 128

type verifyAllResult struct {
	BagID         string
	Name          string
	CreatedAt     time.Time
	TotalPieces   uint32
	DamagedPieces uint32
	Passed        bool
}

func parseVerifyArgs(parts []string) (bagID string, showFiles bool) {
	if len(parts) < 2 {
		return "", false
	}
	if parts[1] == "files" {
		if len(parts) < 3 {
			return "", false
		}
		return parts[2], true
	}
	return parts[1], len(parts) > 2 && strings.EqualFold(parts[2], "files")
}

func parseVerifyAllWorkers(parts []string) (int, error) {
	if len(parts) == 1 {
		return 1, nil
	}
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid arguments")
	}

	workers, err := strconv.Atoi(parts[1])
	if err != nil || workers < 1 {
		return 0, fmt.Errorf("invalid workers count")
	}
	return workers, nil
}

func verify(bagId string, showFiles bool) {
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
		pterm.Error.Println("Bag is unknown")
		return
	}

	report, err := tor.CheckPiecesProofs(context.Background())
	if err != nil {
		pterm.Error.Println("Failed to verify bag:", err.Error())
		return
	}

	pterm.Println("Legend: . ok, ! mismatch, ? no local piece")
	for _, line := range renderVerifyPieceLines(report.Statuses, verifyPiecesLineWidth) {
		pterm.Println(line)
	}

	failedPieces := len(report.Failed)
	switch {
	case failedPieces == 0 && report.MissingPieces == 0 && len(report.MissingFiles) == 0:
		pterm.Success.Println("Result: all pieces are valid and all expected files exist.",
			"Pieces:", report.TotalPieces, "Files:", report.CheckedFiles)
	case failedPieces == 0 && len(report.MissingFiles) == 0:
		pterm.Warning.Println("Result: no mismatches found, but some pieces are missing locally.",
			"OK:", report.OKPieces, "Missing pieces:", report.MissingPieces, "Total:", report.TotalPieces)
	default:
		pterm.Error.Println("Result: verification failed.",
			"OK:", report.OKPieces, "Failed:", failedPieces,
			"Missing pieces:", report.MissingPieces, "Missing files:", len(report.MissingFiles), "Total:", report.TotalPieces)
		if showFiles && failedPieces > 0 {
			files, err := collectVerifyFailedPieceFiles(tor, report)
			if err != nil {
				pterm.Warning.Println("Failed to collect files in corrupted pieces:", err.Error())
			} else if len(files) > 0 {
				pterm.Error.Println("Files in corrupted pieces:")
				for _, path := range files {
					pterm.Println("  " + path)
				}
			}
		}
		if len(report.MissingFiles) > 0 {
			pterm.Error.Println("Missing files:")
			for _, path := range report.MissingFiles {
				pterm.Println("  " + path)
			}
		}
	}
}

func verifyAll(workers int) {
	bags := Storage.GetAll()
	if len(bags) == 0 {
		pterm.Info.Println("No bags to verify")
		return
	}

	if workers < 1 {
		workers = 1
	}
	if workers > len(bags) {
		workers = len(bags)
	}

	progress, _ := termui.StartProgressbar(len(bags), fmt.Sprintf("Verifying %d bags with %d worker(s)...", len(bags), workers))
	defer progress.Stop()

	jobs := make(chan *storage.Torrent, len(bags))
	results := make(chan verifyAllResult, len(bags))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tor := range jobs {
				results <- verifyAllBag(tor)
			}
		}()
	}

	for _, tor := range bags {
		jobs <- tor
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	failed := make([]verifyAllResult, 0)
	passed := 0
	for result := range results {
		progress.Increment()
		if result.Passed {
			passed++
			continue
		}
		failed = append(failed, result)
	}

	sort.Slice(failed, func(i, j int) bool {
		if failed[i].Name != failed[j].Name {
			return failed[i].Name < failed[j].Name
		}
		return failed[i].BagID < failed[j].BagID
	})

	reportPath, err := writeVerifyAllReport(failed)
	if err != nil {
		pterm.Error.Println("verify_all completed, but failed to write report:", err.Error())
		return
	}

	if len(failed) == 0 {
		pterm.Success.Println("verify_all completed.",
			"Total:", len(bags), "Passed:", passed, "Failed:", 0, "Report:", reportPath)
		return
	}

	pterm.Error.Println("verify_all completed.",
		"Total:", len(bags), "Passed:", passed, "Failed:", len(failed), "Report:", reportPath)
}

func verifyAllBag(tor *storage.Torrent) verifyAllResult {
	result := verifyAllResult{
		BagID:     hex.EncodeToString(tor.BagID),
		Name:      bagDisplayName(tor),
		CreatedAt: tor.CreatedAt,
	}
	if tor.Info != nil {
		result.TotalPieces = tor.Info.PiecesNum()
	}

	report, err := tor.CheckPiecesProofs(context.Background())
	if err != nil {
		return result
	}

	result.TotalPieces = report.TotalPieces
	result.DamagedPieces = uint32(len(report.Failed)) + report.MissingPieces
	result.Passed = verifyReportPassed(report)
	return result
}

func verifyReportPassed(report *storage.PieceProofReport) bool {
	if report == nil {
		return false
	}
	return len(report.Failed) == 0 && report.MissingPieces == 0 && len(report.MissingFiles) == 0
}

func writeVerifyAllReport(failed []verifyAllResult) (string, error) {
	reportPath := filepath.Join(*DBPath, fmt.Sprintf("verify_all_report_%s.csv", time.Now().Format("20060102_150405")))
	absPath, err := filepath.Abs(reportPath)
	if err == nil {
		reportPath = absPath
	}

	fl, err := os.Create(reportPath)
	if err != nil {
		return "", err
	}
	defer fl.Close()

	writer := csv.NewWriter(fl)
	rows := buildVerifyAllCSVRows(failed)
	for _, row := range rows {
		if err = writer.Write(row); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		return "", err
	}

	return reportPath, nil
}

func buildVerifyAllCSVRows(failed []verifyAllResult) [][]string {
	rows := make([][]string, 0, len(failed)+1)
	rows = append(rows, []string{"bag_id", "name", "created_at", "total_pieces", "damaged_pieces"})
	for _, result := range failed {
		rows = append(rows, []string{
			result.BagID,
			result.Name,
			formatVerifyAllCreatedAt(result.CreatedAt),
			strconv.FormatUint(uint64(result.TotalPieces), 10),
			strconv.FormatUint(uint64(result.DamagedPieces), 10),
		})
	}
	return rows
}

func formatVerifyAllCreatedAt(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.Format(time.RFC3339)
}

func collectVerifyFailedPieceFiles(tor *storage.Torrent, report *storage.PieceProofReport) ([]string, error) {
	if tor.Header == nil {
		return nil, nil
	}

	filesMap := map[string]struct{}{}
	for _, failed := range report.Failed {
		files, err := tor.GetFilesInPiece(failed.Piece)
		if err != nil {
			return nil, fmt.Errorf("failed to get files for piece %d: %w", failed.Piece, err)
		}
		for _, file := range files {
			filesMap[bagFileLocalPath(tor, file)] = struct{}{}
		}
	}

	files := make([]string, 0, len(filesMap))
	for path := range filesMap {
		files = append(files, path)
	}
	sort.Strings(files)
	return files, nil
}

type bagInfoSnapshot struct {
	Name              string
	Description       string
	DirName           string
	LocalPath         string
	Status            string
	CreatedAt         time.Time
	LastVerifiedAt    time.Time
	VerificationState string
	CreatedLocally    bool
	InfoLoaded        bool
	HeaderLoaded      bool
	DownloadAll       bool
	DownloadOrdered   bool
	ActiveDownload    bool
	ActiveUpload      bool
	Completed         bool
	FilesTotal        int
	FilesSelected     int
	BagDataSize       uint64
	SelectedDataSize  uint64
	DownloadedData    uint64
	HeaderSize        uint64
	BagSize           uint64
	PieceSize         uint32
	TotalPieces       uint32
	DownloadedPieces  int
	PeersCount        int
	DownloadSpeed     uint64
	UploadSpeed       uint64
	UploadedTotal     uint64
	RootHash          string
	HeaderHash        string
}

type bagPeerSnapshot struct {
	ID            string
	Addr          string
	DownloadSpeed uint64
	UploadSpeed   uint64
	Downloaded    uint64
	Uploaded      uint64
}

func info(bagId string) {
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
		pterm.Error.Println("Bag is unknown")
		return
	}

	summary, peers, err := collectBagInfo(tor)
	if err != nil {
		pterm.Error.Println("Failed to collect bag info:", err.Error())
		return
	}

	pterm.Println("Bag info: " + pterm.Cyan(bagId))
	infoTable := pterm.TableData{
		{"Field", "Value"},
		{"Name", summary.Name},
		{"Description", summary.Description},
		{"Created", formatBagInfoTime(summary.CreatedAt)},
		{"Status", summary.Status},
		{"Verification", summary.VerificationState},
		{"Bag ID", bagId},
		{"Local Path", summary.LocalPath},
		{"Directory", summary.DirName},
		{"Files", fmt.Sprintf("%d total / %d selected", summary.FilesTotal, summary.FilesSelected)},
		{"Mode", bagInfoMode(summary.DownloadAll, summary.DownloadOrdered)},
		{"Data", fmt.Sprintf("%s selected / %s total", storage.ToSz(summary.SelectedDataSize), storage.ToSz(summary.BagDataSize))},
		{"Downloaded", fmt.Sprintf("%s / %s", storage.ToSz(summary.DownloadedData), storage.ToSz(summary.SelectedDataSize))},
		{"Header Size", storage.ToSz(summary.HeaderSize)},
		{"Bag Size", storage.ToSz(summary.BagSize)},
		{"Piece Size", storage.ToSz(uint64(summary.PieceSize))},
		{"Pieces", fmt.Sprintf("%d / %d", summary.DownloadedPieces, summary.TotalPieces)},
		{"Peers", fmt.Sprintf("%d", summary.PeersCount)},
		{"Speeds", "D " + storage.ToSpeed(summary.DownloadSpeed) + " | U " + storage.ToSpeed(summary.UploadSpeed)},
		{"Uploaded", storage.ToSz(summary.UploadedTotal)},
		{"Completed", bagInfoYesNo(summary.Completed)},
		{"Created Locally", bagInfoYesNo(summary.CreatedLocally)},
		{"Active", fmt.Sprintf("download %s / upload %s", bagInfoOnOff(summary.ActiveDownload), bagInfoOnOff(summary.ActiveUpload))},
		{"Info Loaded", bagInfoYesNo(summary.InfoLoaded)},
		{"Header Loaded", bagInfoYesNo(summary.HeaderLoaded)},
		{"Root Hash", summary.RootHash},
		{"Header Hash", summary.HeaderHash},
	}
	_ = pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(infoTable).Render()

	if len(peers) == 0 {
		return
	}

	peerTable := pterm.TableData{
		{"Peer ID", "Address", "Download", "Upload", "Downloaded", "Uploaded"},
	}
	for _, peer := range peers {
		peerTable = append(peerTable, []string{
			peer.ID,
			peer.Addr,
			storage.ToSpeed(peer.DownloadSpeed),
			storage.ToSpeed(peer.UploadSpeed),
			storage.ToSz(peer.Downloaded),
			storage.ToSz(peer.Uploaded),
		})
	}

	pterm.Println("Peers (" + pterm.Cyan(fmt.Sprint(len(peers))) + ")")
	_ = pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(peerTable).Render()
}

func collectBagInfo(tor *storage.Torrent) (*bagInfoSnapshot, []bagPeerSnapshot, error) {
	summary := &bagInfoSnapshot{
		Name:            bagDisplayName(tor),
		Description:     bagValueOrDash(""),
		DirName:         bagValueOrDash(""),
		LocalPath:       tor.Path,
		Status:          "Resolving",
		CreatedAt:       tor.CreatedAt,
		CreatedLocally:  tor.CreatedLocally,
		DownloadAll:     tor.IsDownloadAll(),
		DownloadOrdered: tor.IsDownloadOrdered(),
		UploadedTotal:   tor.GetUploadStats(),
		RootHash:        "-",
		HeaderHash:      "-",
	}

	verifyInProgress, lastVerified := tor.GetLastVerifiedAt()
	summary.LastVerifiedAt = lastVerified
	summary.VerificationState = bagVerificationState(verifyInProgress, lastVerified)
	summary.ActiveDownload, summary.ActiveUpload = tor.IsActive()
	if !summary.ActiveDownload {
		summary.Status = "Inactive"
	}

	var activeFilesSize uint64
	if tor.Info != nil {
		summary.InfoLoaded = true
		summary.PieceSize = tor.Info.PieceSize
		summary.HeaderSize = tor.Info.HeaderSize
		summary.BagSize = tor.Info.FileSize
		summary.BagDataSize = tor.Info.FileSize - tor.Info.HeaderSize
		summary.SelectedDataSize = summary.BagDataSize
		summary.TotalPieces = tor.Info.PiecesNum()
		summary.DownloadedPieces = tor.DownloadedPiecesNum()
		summary.RootHash = hex.EncodeToString(tor.Info.RootHash)
		summary.HeaderHash = hex.EncodeToString(tor.Info.HeaderHash)
		summary.Description = bagValueOrDash(tor.Info.Description.Value)

		downloadedPieces := summary.DownloadedPieces
		downloaded := uint64(downloadedPieces*int(tor.Info.PieceSize)) - tor.Info.HeaderSize
		if uint64(downloadedPieces*int(tor.Info.PieceSize)) < tor.Info.HeaderSize {
			downloaded = 0
		}
		if downloaded > summary.BagDataSize {
			downloaded = summary.BagDataSize
		}

		summary.Completed = uint32(downloadedPieces) == tor.Info.PiecesNum()
		if !summary.Completed && !tor.IsDownloadAll() {
			mask := tor.PiecesMask()
			summary.Completed = true
			for _, f := range tor.GetActiveFilesIDs() {
				off, err := tor.GetFileOffsetsByID(f)
				if err != nil {
					continue
				}
				activeFilesSize += off.Size

				if summary.Completed {
					for pc := off.FromPiece; pc <= off.ToPiece; pc++ {
						if !bagHasPiece(mask, pc) {
							summary.Completed = false
							break
						}
					}
				}
			}

			summary.SelectedDataSize = activeFilesSize
			if downloaded > activeFilesSize {
				downloaded = activeFilesSize
			}
		}
		summary.DownloadedData = downloaded

		if verifyInProgress {
			summary.Status = "Verifying"
		} else if summary.Completed {
			summary.Status = "Downloaded"
			if summary.ActiveUpload {
				summary.Status = "Seeding"
			}
		} else if summary.ActiveDownload {
			if len(tor.GetActiveFilesIDs()) == 0 && !tor.IsDownloadAll() {
				summary.Status = "Header downloaded"
			} else {
				summary.Status = "Downloading"
			}
		}
	}

	if tor.Header != nil {
		summary.HeaderLoaded = true
		dirName := strings.TrimSuffix(string(tor.Header.DirName), "/")
		summary.DirName = bagValueOrDash(dirName)
		summary.FilesTotal = int(tor.Header.FilesCount)
		if tor.IsDownloadAll() {
			summary.FilesSelected = int(tor.Header.FilesCount)
		} else {
			summary.FilesSelected = len(tor.GetActiveFilesIDs())
		}
		summary.LocalPath = bagLocalPath(tor)
		summary.Name = bagDisplayName(tor)

		if summary.SelectedDataSize == 0 && tor.Info != nil && !tor.IsDownloadAll() {
			for _, f := range tor.GetActiveFilesIDs() {
				off, err := tor.GetFileOffsetsByID(f)
				if err != nil {
					continue
				}
				summary.SelectedDataSize += off.Size
			}
		}
	}

	if !summary.InfoLoaded {
		summary.Description = "-"
	}
	if summary.SelectedDataSize == 0 && summary.InfoLoaded && summary.DownloadAll {
		summary.SelectedDataSize = summary.BagDataSize
	}

	peersMap := tor.GetPeers()
	summary.PeersCount = len(peersMap)

	peers := make([]bagPeerSnapshot, 0, len(peersMap))
	for id, peer := range peersMap {
		summary.DownloadSpeed += peer.GetDownloadSpeed()
		summary.UploadSpeed += peer.GetUploadSpeed()
		peers = append(peers, bagPeerSnapshot{
			ID:            id,
			Addr:          peer.Addr,
			DownloadSpeed: peer.GetDownloadSpeed(),
			UploadSpeed:   peer.GetUploadSpeed(),
			Downloaded:    peer.Downloaded,
			Uploaded:      peer.Uploaded,
		})
	}

	sort.Slice(peers, func(i, j int) bool {
		left := peers[i].DownloadSpeed + peers[i].UploadSpeed
		right := peers[j].DownloadSpeed + peers[j].UploadSpeed
		if left != right {
			return left > right
		}
		if peers[i].Downloaded+peers[i].Uploaded != peers[j].Downloaded+peers[j].Uploaded {
			return peers[i].Downloaded+peers[i].Uploaded > peers[j].Downloaded+peers[j].Uploaded
		}
		return peers[i].ID < peers[j].ID
	})

	return summary, peers, nil
}

func bagDisplayName(tor *storage.Torrent) string {
	if tor.Header != nil {
		if dir := strings.TrimSuffix(string(tor.Header.DirName), "/"); dir != "" {
			return dir
		}

		if tor.Header.FilesCount == 1 {
			if file, err := tor.GetFileOffsetsByID(0); err == nil && file.Name != "" {
				return file.Name
			}
		}
	}

	if tor.Info != nil && tor.Info.Description.Value != "" {
		return tor.Info.Description.Value
	}
	return "-"
}

func bagLocalPath(tor *storage.Torrent) string {
	if tor.Header == nil {
		return tor.Path
	}

	dir := strings.TrimSuffix(string(tor.Header.DirName), "/")
	if dir == "" {
		if tor.Header.FilesCount == 1 {
			if file, err := tor.GetFileOffsetsByID(0); err == nil && file.Name != "" {
				return filepath.Join(tor.Path, file.Name)
			}
		}
		return tor.Path
	}
	return filepath.Join(tor.Path, dir)
}

func bagFileLocalPath(tor *storage.Torrent, file *storage.FileInfo) string {
	if tor.Header == nil || file == nil {
		return tor.Path
	}
	return filepath.Join(bagLocalPath(tor), file.Name)
}

func bagInfoMode(downloadAll, downloadOrdered bool) string {
	mode := "selected files"
	if downloadAll {
		mode = "all files"
	}
	if downloadOrdered {
		return mode + ", ordered"
	}
	return mode + ", parallel"
}

func bagVerificationState(inProgress bool, ts time.Time) string {
	if inProgress {
		return "in progress"
	}
	if ts.IsZero() {
		return "never"
	}
	return formatBagInfoTime(ts)
}

func formatBagInfoTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.Format("2006-01-02 15:04:05 MST")
}

func bagValueOrDash(v string) string {
	if strings.TrimSpace(v) == "" {
		return "-"
	}
	return v
}

func bagInfoYesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

func bagInfoOnOff(v bool) string {
	if v {
		return "on"
	}
	return "off"
}

func bagHasPiece(mask []byte, piece uint32) bool {
	if int(piece/8) >= len(mask) {
		return false
	}
	return mask[piece/8]&(1<<(piece%8)) != 0
}

func renderVerifyPieceLines(statuses []storage.PieceProofStatus, width int) []string {
	if width <= 0 {
		width = verifyPiecesLineWidth
	}
	if len(statuses) == 0 {
		return nil
	}

	digits := len(fmt.Sprint(len(statuses) - 1))
	lines := make([]string, 0, (len(statuses)+width-1)/width)
	for start := 0; start < len(statuses); start += width {
		end := start + width
		if end > len(statuses) {
			end = len(statuses)
		}

		var line strings.Builder
		if len(statuses) > width {
			line.WriteString(fmt.Sprintf("%*d-%*d ", digits, start, digits, end-1))
		}
		line.WriteByte('[')
		for _, status := range statuses[start:end] {
			line.WriteByte(renderVerifyPieceChar(status))
		}
		line.WriteByte(']')
		lines = append(lines, line.String())
	}
	return lines
}

func renderVerifyPieceChar(status storage.PieceProofStatus) byte {
	switch status {
	case storage.PieceProofStatusOK:
		return '.'
	case storage.PieceProofStatusMismatch:
		return '!'
	case storage.PieceProofStatusMissing:
		return '?'
	default:
		return '?'
	}
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
