package config

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"github.com/pterm/pterm"
	"log"
	"net"
	"os"
	"runtime"
	"time"
)

type ChannelConfig struct {
	VirtualChannelProxyFee      string
	QuarantineDurationSec       uint32
	MisbehaviorFine             string
	ConditionalCloseDurationSec uint32
}

type PaymentsConfig struct {
	Enabled            bool
	PaymentsServerKey  []byte
	WalletPrivateKey   []byte
	PaymentsListenAddr string
	DBPath             string
	SecureProofPolicy  bool
	ChannelConfig      ChannelConfig
}

type PaymentChain struct {
	NodeKey     []byte
	Fee         string
	MaxCapacity string
}

type TunnelSectionPayment struct {
	Chain              []PaymentChain
	PricePerPacketNano uint64
}

type TunnelRouteSection struct {
	Key     []byte
	Payment *TunnelSectionPayment
}

type TunnelConfig struct {
	Enabled         bool
	TunnelServerKey []byte
	TunnelThreads   uint
	Payments        PaymentsConfig
	OutGateway      TunnelRouteSection
	RouteOut        []TunnelRouteSection
	RouteIn         []TunnelRouteSection
}

type Config struct {
	Key              ed25519.PrivateKey
	ListenAddr       string
	ExternalIP       string
	DownloadsPath    string
	NetworkConfigUrl string
	Tunnel           TunnelConfig
}

func checkIPAddress(ip string) string {
	p := net.ParseIP(ip)
	if p == nil {
		log.Println("bad ip", len(p))
		return ""
	}
	p = p.To4()
	if p == nil {
		log.Println("bad ip, not v4", len(p))
		return ""
	}

	return p.String()
}

func checkCanSeed() (string, bool) {
	ch := make(chan bool, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ip := ""
	go func() {
		defer func() {
			ch <- ip != ""
		}()

		listen, err := net.Listen("tcp", "0.0.0.0:18889")
		if err != nil {
			log.Println("listen err", err.Error())
			return
		}
		defer listen.Close()

		conn, err := listen.Accept()
		if err != nil {
			log.Println("accept err", err.Error())
			return
		}

		ipData := make([]byte, 256)
		n, err := conn.Read(ipData)
		if err != nil {
			log.Println("read err", err.Error())
			return
		}

		ip = string(ipData[:n])
		ip = checkIPAddress(ip)
		_ = conn.Close()
	}()

	sp, _ := pterm.DefaultSpinner.Start("Resolving port checker...")
	ips, err := net.LookupIP("tonutils.com")
	if err != nil || len(ips) == 0 {
		sp.Fail("Port is not resolved, you can download, but no-one can download from you, unless you specify your ip manually in config.json")
		return "", false
	}
	sp.Success("Port checker resolved.")

	sp, _ = pterm.DefaultSpinner.Start("Using port checker tonutils.com at ", ips[0].String())
	conn, err := net.Dial("tcp", ips[0].String()+":9099")
	if err != nil {
		return "", false
	}

	_, err = conn.Write([]byte("ME"))
	if err != nil {
		return "", false
	}
	ok := false
	select {
	case k := <-ch:
		ok = k
		sp.Success("Ports are open, public ip is ", ip, " Seeding is available, bags can be downloaded from you.")
	case <-ctx.Done():
		_ = sp.Stop()
		pterm.Warning.Println("No request from port checker, looks like it cannot reach you, so ports are probably closed. You can download, " +
			"but no-one can download from you, unless you specify your ip manually in db's config.json")
	}

	return ip, ok
}

func LoadConfig(dir string) (*Config, error) {
	_, err := os.Stat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(dir, os.ModePerm)
		}
		if err != nil {
			return nil, err
		}
	}

	path := dir + "/config.json"
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		_, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, err
		}

		_, paymentsPrv, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, err
		}

		_, tunnelPrv, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, err
		}

		cfg := &Config{
			Key:              priv,
			ListenAddr:       "0.0.0.0:17555",
			ExternalIP:       "",
			DownloadsPath:    "./downloads/",
			NetworkConfigUrl: "https://ton-blockchain.github.io/global.config.json",
			Tunnel: TunnelConfig{
				Enabled:         false,
				TunnelServerKey: tunnelPrv.Seed(),
				TunnelThreads:   uint(runtime.NumCPU()),
				Payments: PaymentsConfig{
					Enabled:            false,
					PaymentsServerKey:  paymentsPrv.Seed(),
					WalletPrivateKey:   priv.Seed(),
					PaymentsListenAddr: "0.0.0.0:17331",
					DBPath:             "./payments-db/",
					SecureProofPolicy:  false,
					ChannelConfig: ChannelConfig{
						VirtualChannelProxyFee:      "0.01",
						QuarantineDurationSec:       600,
						MisbehaviorFine:             "0.15",
						ConditionalCloseDurationSec: 180,
					},
				},
				OutGateway: TunnelRouteSection{
					Key: nil,
					Payment: &TunnelSectionPayment{
						Chain: []PaymentChain{
							{
								NodeKey:     nil,
								Fee:         "0.005",
								MaxCapacity: "3",
							},
						},
						PricePerPacketNano: 0,
					},
				},
				RouteOut: []TunnelRouteSection{},
				RouteIn:  []TunnelRouteSection{},
			},
		}

		ip, seed := checkCanSeed()
		if seed {
			cfg.ExternalIP = ip
		}

		err = SaveConfig(cfg, dir)
		if err != nil {
			return nil, err
		}

		return cfg, nil
	} else if err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		var cfg Config
		err = json.Unmarshal(data, &cfg)
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}

	return nil, err
}

func SaveConfig(cfg *Config, dir string) error {
	path := dir + "/config.json"

	data, err := json.MarshalIndent(cfg, "", "\t")
	if err != nil {
		return err
	}

	err = os.WriteFile(path, data, 0766)
	if err != nil {
		return err
	}
	return nil
}
