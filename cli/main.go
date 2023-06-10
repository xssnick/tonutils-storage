package main

import (
	"context"
	"encoding/hex"
	"flag"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-storage/config"
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/storage"
	"math/bits"
	"net"
	"os"
	"strings"
)

var (
	DBPath = flag.String("db", "", "Path to db folder")
)

var Storage *db.Storage
var Connector storage.NetConnector

func main() {
	flag.Parse()

	storage.Logger = func(v ...any) {}
	adnl.Logger = func(v ...any) {}
	dht.Logger = func(v ...any) {}

	_ = pterm.DefaultBigText.WithLetters(
		putils.LettersFromStringWithStyle("Ton", pterm.FgBlue.ToStyle()),
		putils.LettersFromStringWithStyle("Utils", pterm.FgLightBlue.ToStyle())).
		Render()

	pterm.DefaultBox.WithBoxStyle(pterm.NewStyle(pterm.FgLightBlue)).Println(pterm.LightWhite("   Storage   "))

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

	lsCfg, err := liteclient.GetConfigFromUrl(context.Background(), "https://ton-blockchain.github.io/global.config.json")
	if err != nil {
		pterm.Error.Println("Failed to download ton config:", err.Error())
		os.Exit(1)
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

	dhtClient, err := dht.NewClientFromConfig(context.Background(), dhtGate, lsCfg)
	if err != nil {
		pterm.Error.Println("Failed to init dht client:", err.Error())
		os.Exit(1)
	}

	downloadGate := adnl.NewGateway(cfg.Key)
	if err = downloadGate.StartClient(); err != nil {
		pterm.Error.Println("Failed to init dht downloader gateway:", err.Error())
		os.Exit(1)
	}

	srv := storage.NewServer(dhtClient, gate, cfg.Key, serverMode)
	Connector = storage.NewConnector(srv)

	Storage, err = db.NewStorage(ldb, Connector, false)
	if err != nil {
		pterm.Error.Println("Failed to init storage:", err.Error())
		os.Exit(1)
	}
	srv.SetStorage(Storage)

	pterm.Info.Println("If you use it for commercial purposes please consider", pterm.LightWhite("donation")+". It allows us to develop such products 100% free.")
	pterm.Info.Println("We also have telegram group if you have some questions.", pterm.LightBlue("https://t.me/tonrh"))

	pterm.Success.Println("Storage started")
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
			download(parts[1], downloadGate)
		case "create":
			if len(parts) < 3 {
				pterm.Error.Println("Usage: create [path] [description]")
				continue
			}
			create(parts[1], parts[2])
		default:
			fallthrough
		case "help":
			pterm.Info.Println("Commands:\n"+
				"create [path] [description]\n",
				"download [bag_id]\n",
				"help\n",
			)
		}
	}
}

func download(bagId string, gate *adnl.Gateway) {
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
		if err = tor.Start(true); err != nil {
			pterm.Error.Println("Failed to start:", err.Error())
			return
		}
	}

	report := make(chan storage.Event, 100)
	sp, _ := pterm.DefaultSpinner.WithText("Resolving bag information...").Start()

	var res *storage.DownloadResult
	var dowProgress *pterm.ProgressbarPrinter
	for {
		e := <-report

		switch e.Name {
		case storage.EventErr:
			if sp != nil {
				sp.Fail()
			}
			pterm.Error.Println("Failed to download:", e.Value)
		case storage.EventBagResolved:
			sp.Success("Bag information resolved")
			sp = nil

			x := e.Value.(storage.PiecesInfo)
			dowProgress, _ = pterm.DefaultProgressbar.WithTotal(x.OverallPieces).WithTitle("Downloading bag").Start()
			for i := 0; i < x.OverallPieces-x.PiecesToDownload; i++ {
				dowProgress.Increment()
			}

			err = Storage.SetTorrent(tor)
			if err != nil {
				pterm.Error.Println("Failed to add bag:", err.Error())
				return
			}
		case storage.EventDone:
			v := e.Value.(storage.DownloadResult)
			res = &v
		case storage.EventFileDownloaded:
			pterm.Success.Println("Downloaded", e.Value)
		case storage.EventPieceDownloaded:
			dowProgress.Increment()
		case storage.EventProgress:
			p := e.Value.(storage.Progress)

			var listPeers []string
			peers := tor.GetPeers()
			for s := range peers {
				listPeers = append(listPeers, s)
			}
			dowProgress.UpdateTitle("Downloading " + p.Speed + ". Downloaded " + p.Downloaded + " (" + strings.Join(listPeers, ", ") + ")")
		}

		if res != nil {
			break
		}
	}
}

func create(path, name string) {
	it, err := storage.CreateTorrent(path, name, Storage, Connector)
	if err != nil {
		pterm.Error.Println("Failed to create bag:", err.Error())
		return
	}
	it.Start(true)

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
		{"Bag ID", "Description", "Downloaded", "Size"},
	}

	for _, t := range Storage.GetAll() {
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

		table = append(table, []string{hex.EncodeToString(t.BagID), t.Info.Description.Value, storage.ToSz(downloaded), storage.ToSz(full)})
	}

	if len(table) > 1 {
		pterm.Println("Active bags")
		pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(table).Render()
	}
}
