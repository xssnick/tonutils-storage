module github.com/xssnick/tonutils-storage

go 1.19

require (
	github.com/pterm/pterm v0.12.59
	github.com/rs/zerolog v1.30.0
	github.com/syndtr/goleveldb v1.0.0
	github.com/xssnick/ton-payment-network v0.0.0-20231124104742-f15e43a60956
	github.com/xssnick/tonutils-go v1.8.6-0.20231201103308-17dded539920
)

require (
	atomicgo.dev/cursor v0.1.1 // indirect
	atomicgo.dev/keyboard v0.2.9 // indirect
	github.com/containerd/console v1.0.3 // indirect
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/gookit/color v1.5.3 // indirect
	github.com/lithammer/fuzzysearch v1.1.5 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/oasisprotocol/curve25519-voi v0.0.0-20220328075252-7dd334e3daae // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/sigurn/crc16 v0.0.0-20211026045750-20ab5afb07e3 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	golang.org/x/crypto v0.0.0-20220321153916-2c7772ba3064 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/term v0.6.0 // indirect
	golang.org/x/text v0.9.0 // indirect
)

replace github.com/xssnick/ton-payment-network v0.0.0-20231124104742-f15e43a60956 => ../payment-network/
