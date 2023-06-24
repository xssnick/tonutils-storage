package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-storage/api"
	"github.com/xssnick/tonutils-storage/config"
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/storage"
	"log"
	"math/bits"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	API                 = flag.String("api", "", "HTTP API listen address")
	CredentialsLogin    = flag.String("api-login", "", "HTTP API credentials login")
	CredentialsPassword = flag.String("api-password", "", "HTTP API credentials password")
	DBPath              = flag.String("db", "tonutils-storage-db", "Path to db folder")
	Verbosity           = flag.Int("debug", 0, "Debug logs")
	IsDaemon            = flag.Bool("daemon", false, "Daemon mode, no command line input")
)

var GitCommit string

var Storage *db.Storage
var Connector storage.NetConnector

func main() {
	flag.Parse()

	storage.Logger = func(v ...any) {}
	adnl.Logger = func(v ...any) {}
	dht.Logger = func(v ...any) {}

	if *Verbosity > 3 {
		*Verbosity = 3
	}

	switch *Verbosity {
	case 3:
		adnl.Logger = log.Println
		fallthrough
	case 2:
		dht.Logger = log.Println
		fallthrough
	case 1:
		storage.Logger = log.Println
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

	lsCfg, err := liteclient.GetConfigFromUrl(context.Background(), "https://ton.org/global.config.json")
	if err != nil {
		pterm.Warning.Println("Failed to download ton config:", err.Error(), "; We will take it from static cache")
		lsCfg = &liteclient.GlobalConfig{}
		if err = json.NewDecoder(bytes.NewBufferString(config.FallbackNetworkConfig)).Decode(lsCfg); err != nil {
			pterm.Error.Println("Failed to parse fallback ton config:", err.Error())
			os.Exit(1)
		}
	}

	gate := adnl.NewGateway(cfg.Key)

	serverMode := ip != nil
	if serverMode {
		gate.SetExternalIP(ip)
		err = gate.StartServer(cfg.ListenAddr)
		if err != nil {
			pterm.Error.Println("Failed to start adnl gateway in server mode:", err.Error())
			os.Exit(1)
		}
	} else {
		err = gate.StartClient()
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
		pterm.Error.Println("Failed to init dht downloader gateway:", err.Error())
		os.Exit(1)
	}

	srv := storage.NewServer(dhtClient, gate, cfg.Key, serverMode, true)
	Connector = storage.NewConnector(srv)

	Storage, err = db.NewStorage(ldb, Connector, true)
	if err != nil {
		pterm.Error.Println("Failed to init storage:", err.Error())
		os.Exit(1)
	}
	srv.SetStorage(Storage)

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
				cmd, err := pterm.DefaultInteractiveTextInput.Show("Command:")
				if err != nil {
					panic(err)
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
				default:
					fallthrough
				case "help":
					pterm.Info.Println("Commands:\n"+
						"create [path] [description]\n",
						"download [bag_id]\n",
						"remove [bag_id] [with files? (true/false)]\n",
						"list\n",
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

	it, err := storage.CreateTorrent(context.Background(), rootPath, dirName, name, Storage, Connector, files)
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
		{"Bag ID", "Description", "Downloaded", "Size", "Peers", "Download", "Upload", "Completed"},
	}

	for _, t := range Storage.GetAll() {
		var strDownloaded, strFull, description = "0 Bytes", "???", "???"
		completed := false
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
			completed = downloaded == full

			strDownloaded = storage.ToSz(downloaded)
			strFull = storage.ToSz(full)
			description = t.Info.Description.Value
		}

		var dow, upl, num uint64
		for _, p := range t.GetPeers() {
			dow += p.GetDownloadSpeed()
			upl += p.GetUploadSpeed()
			num++
		}

		table = append(table, []string{hex.EncodeToString(t.BagID), description,
			strDownloaded, strFull, fmt.Sprint(num),
			storage.ToSpeed(dow), storage.ToSpeed(upl), fmt.Sprint(completed)})
	}

	if len(table) > 1 {
		pterm.Println("Active bags")
		pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(table).Render()
	}
}
