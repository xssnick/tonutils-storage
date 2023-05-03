package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/storage"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-storage/config"
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/downloader"
	"github.com/xssnick/tonutils-storage/server"
	"github.com/xssnick/tonutils-storage/torrent"
	"net"
	"os"
	"strings"
)

var (
	DBPath = flag.String("db", "", "Path to db folder")
)

var Downloader *downloader.Downloader
var Storage *db.Storage

func main() {
	flag.Parse()

	storage.Logger = func(v ...any) {}
	adnl.Logger = func(v ...any) {}
	dht.Logger = func(v ...any) {}

	_ = pterm.DefaultBigText.WithLetters(
		putils.LettersFromStringWithStyle("Ton", pterm.FgBlue.ToStyle()),
		putils.LettersFromStringWithStyle("Utils", pterm.FgLightBlue.ToStyle())).
		Render()

	pterm.DefaultHeader.Println("Storage")
	if *DBPath == "" {
		pterm.Error.Println("DB path should be specified with -db flag")
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*DBPath)
	if err != nil {
		pterm.Error.Println("Failed to load config:", err.Error())
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
	err = gate.StartClient()
	if err != nil {
		pterm.Error.Println("Failed to start dht client gateway:", err.Error())
		os.Exit(1)
	}

	dhtClient, err := dht.NewClientFromConfig(context.Background(), gate, lsCfg)
	if err != nil {
		pterm.Error.Println("Failed to init dht client:", err.Error())
		os.Exit(1)
	}
	Downloader = downloader.NewDownloader(dhtClient)

	Storage, err = db.NewStorage(*DBPath)
	if err != nil {
		pterm.Error.Println("Failed to init storage:", err.Error())
		os.Exit(1)
	}

	list()

	err = server.NewServer(Storage, dhtClient, cfg.Key, cfg.ListenAddr, ip)
	if err != nil {
		pterm.Error.Println("Failed to start adnl server:", err.Error())
		os.Exit(1)
	}

	pterm.Success.Println("Storage started")

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

	res, err := Downloader.Download(context.Background(), *DBPath+"/downloads/", bag)
	if err != nil {
		// pterm.Error.Println("Failed to download:", err.Error())
		return
	}

	it, err := torrent.CreateTorrent(res.Path, res.Description)
	if err != nil {
		pterm.Error.Println("Failed to create torrent:", err.Error())
		return
	}

	if bytes.Equal(it.BagID, bag) {
		err = Storage.AddTorrent(it)
		if err != nil {
			pterm.Error.Println("Failed to add torrent:", err.Error())
			return
		}
		list()
	} else {
		pterm.Error.Println("Validation failed, incorrect final hash.")
		return
	}
}

func create(path, name string) {
	it, err := torrent.CreateTorrent(path, name)
	if err != nil {
		pterm.Error.Println("Failed to create torrent:", err.Error())
		return
	}

	err = Storage.AddTorrent(it)
	if err != nil {
		pterm.Error.Println("Failed to add torrent:", err.Error())
		return
	}

	pterm.Success.Println("Torrent created and ready:", pterm.Cyan(hex.EncodeToString(it.BagID)))
	list()
}

func list() {
	var table = pterm.TableData{
		{"Bag ID", "Description", "Size"},
	}

	for _, t := range Storage.GetAll() {
		table = append(table, []string{hex.EncodeToString(t.BagID), t.Info.Description.Value, downloader.ToSz(t.Info.FileSize - t.Info.HeaderSize)})
	}

	if len(table) > 1 {
		pterm.Info.Println("Active bags:")
		pterm.DefaultTable.WithHasHeader().WithBoxed().WithData(table).Render()
	}
}
