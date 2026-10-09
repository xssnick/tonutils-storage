# Tonutils Storage
[![Based on TON][ton-svg]][ton] [![Join our group][join-svg]][tg]

Alternative TON Storage implementation based on [tonutils-go](https://github.com/xssnick/tonutils-go), with [HTTP API](#http-api).

You could freely use this storage in any type of projects.

If you want to support this project and help us to develop projects like this 100% free and open source, we would be grateful if you donate any amount of TON or Jettons to `EQBx6tZZWa2Tbv6BvgcvegoOQxkRrVaBVwBOoW85nbP37_Go` ☺️

<img width="1082" alt="Screen Shot 2023-06-12 at 20 22 54" src="https://github.com/xssnick/tonutils-storage/assets/9332353/c321230b-0a6c-462d-946d-66d31bdb588a">

## Quick start

Download the binary for your platform:

* [Linux AMD64](https://github.com/xssnick/tonutils-storage/releases/latest/download/tonutils-storage-linux-amd64)
* [Linux ARM64](https://github.com/xssnick/tonutils-storage/releases/latest/download/tonutils-storage-linux-arm64)
* [Windows x64](https://github.com/xssnick/tonutils-storage/releases/latest/download/tonutils-storage-x64.exe)
* [Mac Intel](https://github.com/xssnick/tonutils-storage/releases/latest/download/tonutils-storage-mac-amd64)
* [Mac Apple Silicon](https://github.com/xssnick/tonutils-storage/releases/latest/download/tonutils-storage-mac-arm64)

On Linux/macOS, rename the downloaded file to `tonutils-storage`, make it
executable, and run it:

```sh
chmod +x tonutils-storage
./tonutils-storage
```

On Windows, run the downloaded `.exe`. At the interactive prompt, add a bag and
check its progress:

```text
download 85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f
list
```

This README describes the current source tree. Published binaries may not yet
include changes from unreleased commits.

### Build from source

Requires Go 1.26.0 or later. From the repository directory:

```sh
make build
./build/tonutils-storage
```

## CLI

Commands are entered at the interactive prompt:

| Command | Purpose |
| --- | --- |
| `create <path> <description>` | Create a bag from a file or directory and enable seeding |
| `download <bag_id>` | Add or resume downloading all files in a bag |
| `info <bag_id>` | Display bag, file, and peer details |
| `verify <bag_id> [files]` | Verify local pieces; `files` also lists files in damaged pieces |
| `verify_all [workers]` | Verify all bags and write a CSV report in the DB directory when bags exist; default: one worker |
| `remove <bag_id> <with_files>` | Remove a bag; `with_files` is `true` or `false`; `true` also removes its content files |
| `list` | Display bags and transfer progress |
| `providers <bag_id> <owner_address>` | Display storage providers |
| `rent-storage <bag_id> <owner_address> <provider_id> <amount>` | Prepare a storage rental transaction |
| `rent-withdraw <bag_id> <owner_address> <amount>` | Prepare a withdrawal transaction |
| `rent-topup <bag_id> <owner_address> <amount>` | Prepare a top-up transaction |
| `help` | Display commands |

Amounts are in TON. Rental commands print wallet transaction links.
Interactive arguments are separated by spaces; quoting and arguments containing
spaces are not supported. Use the HTTP API for paths or descriptions containing
spaces. `--daemon` disables the interactive prompt.

Run `./tonutils-storage --help` for startup flags:

| Flag | Default / behavior |
| --- | --- |
| `--db <path>` | `tonutils-storage-db`; directory for database and configuration |
| `--daemon` | `false`; disable interactive input |
| `--api <ip:port>` | Empty; HTTP API disabled |
| `--api-login`, `--api-password` | Both must be supplied or both omitted |
| `--network-config <path>` | Empty; otherwise load TON network configuration from this file |
| `--no-verify` | `false`; skip startup integrity verification |
| `--no-remove` | `false`; protect content files from deletion; generated piece caches can still be removed |
| `--threads <n>` | `0`; choose the CPU count, capped at 80; positive values override it |
| `--fd-cache-limit <n>` | `800`; positive values set the descriptor cache size |
| `--force-piece-size <bytes>` | `0`; automatically choose piece size when creating bags |
| `--enable-tunnel` | `false`; enable tunnel mode using tunnel configuration |
| `--dht-parallelism <n>` | `20`; base DHT concurrency; values below 1 become 1 |
| `--pprof-addr <ip:port>` | Empty; pprof disabled |
| `--limit-download <bytes/s>`, `--limit-upload <bytes/s>` | `0`; no limit imposed by the flag; positive values set the respective limit |
| `--verbosity <n>` | `2`; logging verbosity |
| `--version` | Print build version and exit |

### Configuration and seeding

Configuration is stored in `<db>/config.json`, by default
`tonutils-storage-db/config.json`. A new configuration uses
`ListenAddr: "0.0.0.0:17555"` and `DownloadsPath: "./downloads/"`.

When creating the configuration, storage tries to discover its public IPv4
address using the `tonutils.com` port checker. This check uses TCP port 18889;
it does not verify reachability of the storage UDP port.

If discovery fails, set the `ExternalIP` field to your public IPv4 address,
ensure the UDP port configured in `ListenAddr` is reachable from the Internet,
and restart storage. Forward that UDP port when using a router. Downloads remain
available with an empty `ExternalIP`. Tunnel mode requires its own configuration.

Completed bags with seeding enabled use a separate DHT refresh pool, ordered by
update deadline. Its concurrency scales with the number of seeds and observed
Store duration, up to 512 workers, so downloads cannot occupy its slots.
`--dht-parallelism` sets the base concurrency for bag searches, address lookups,
and session initialization. The seed refresh target is four minutes; DHT failures
or latency beyond the pool's capacity can delay updates.

### Resource requirements

RAM and CPU usage depend on the number of bags, piece sizes, connected peers,
and concurrent transfers or verification jobs. Plan capacity for your workload.
Allocate disk space for the selected content, database, and generated piece
caches, and provide Internet connectivity.

## HTTP API

Enable the API with `--api <ip:port>`. To use HTTP Basic Auth, set both
`--api-login` and `--api-password`:

```sh
./tonutils-storage --daemon --api 127.0.0.1:8192 --api-login admin --api-password password
```

```sh
curl --user admin:password 'http://127.0.0.1:8192/api/v1/list?limit=100'
```

Handler responses use `Content-Type: application/json`. Send JSON request bodies
with `Content-Type: application/json`. All routes require Basic Auth when it is
configured. Incorrect or missing credentials return HTTP 401:

```json
{"error":"Invalid credentials"}
```

Bag and provider IDs are 32 bytes represented as 64 hexadecimal characters.
Input IDs may use either case; response IDs use lowercase. Invalid IDs or JSON
bodies return HTTP 400 with `{"error":"..."}`. Other handler errors generally use
this same shape. Examples below illustrate the JSON structure and use sample
values; replace IDs and paths with your own.

The methods below describe intended usage. Currently only `/verify` enforces
its HTTP method; it returns HTTP 405 for methods other than POST.

#### POST /api/v1/add

Add or resume a bag. `path` is optional and defaults to `DownloadsPath`.
For a new bag, its storage directory is `<path>/<lowercase-bag-id>`; for an
existing bag, `path` is ignored.

`download_all` defaults to `false`. For a new bag, `false` with omitted or empty
`files` downloads only the header. For an existing bag, omitted or empty `files`
preserves its stored file indices; `download_all` still takes the requested value,
including the default `false`. A nonempty `files` array replaces the selection
and sets `download_all=false`, even if the request specifies `true`.

Load the header first, read file indices from `/details`, then call `/add` with
the desired indices. To download every file, use `download_all=true` and omit
`files` or pass an empty array.

Request:

```json
{
  "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
  "path": "/root/downloads",
  "files": [0, 1, 2],
  "download_all": false
}
```

Response, HTTP 200:

```json
{"ok":true}
```

`ok=true` means the request was accepted; downloading continues asynchronously.
The bag is started with upload enabled.

#### GET /api/v1/list

Optional query parameters: `limit` (positive integer) and `offset` (nonnegative
integer). `/api/v1/list?limit=100&offset=200` returns up to 100 bags after skipping
200. `offset` may be used without `limit`. Pages are ordered by creation time,
newest first, with bag ID ascending when timestamps match. Adding or removing
bags between requests can shift offset pages.

Without these parameters, the endpoint returns the full list. Empty, duplicate,
negative, overflowing, or otherwise invalid parameter values return HTTP 400;
`limit=0` is also invalid. An empty paginated result is `{"bags":[]}`. For backward
compatibility, an empty list without either parameter is `{"bags":null}`.

Response, HTTP 200:

```json
{
  "bags": [
    {
      "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
      "description": "FunnyPack",
      "downloaded": 130936,
      "size": 188249739,
      "header_size": 136,
      "peers": 1,
      "download_speed": 0,
      "upload_speed": 0,
      "files_count": 3,
      "dir_name": "video/",
      "completed": false,
      "header_loaded": true,
      "info_loaded": true,
      "active": true,
      "seeding": true,
      "download_all": true
    }
  ]
}
```

Sizes are in bytes; speeds are in bytes per second.

| Field | Meaning |
| --- | --- |
| `size` | Total content size, excluding the header, regardless of file selection |
| `header_size` | Serialized bag header size |
| `downloaded` | Estimate from available pieces, excluding the header; partial selection may cap it to selected file sizes |
| `completed` | All pieces are available, or the current partial file selection is satisfied |
| `header_loaded`, `info_loaded` | Header / torrent metadata are available locally |
| `active` | Bag is started; it need not be transferring bytes at this moment |
| `seeding` | Bag is started, upload is enabled, and the header is loaded |
| `download_all` | All files are selected for download |
| `peers` | Number of tracked bag peers |

#### GET /api/v1/details?bag_id=<id>

Returns the list fields plus piece metadata, storage path, files, and peer
details. Here `peers` is an array instead of the count returned by `/list`.

Response, HTTP 200:

```json
{
  "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
  "description": "FunnyPack",
  "downloaded": 130936,
  "size": 188249739,
  "header_size": 136,
  "download_speed": 0,
  "upload_speed": 0,
  "files_count": 3,
  "dir_name": "video/",
  "completed": false,
  "header_loaded": true,
  "info_loaded": true,
  "active": true,
  "seeding": true,
  "download_all": true,
  "bag_pieces_num": 1437,
  "has_pieces_mask": "AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "piece_size": 131072,
  "bag_size": 188249875,
  "merkle_hash": "acaaf3306ce628b18c62bd074b263c2354b1fd156eab189d4398db02f40ed09c",
  "path": "/root/downloads/85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
  "files": [
    {"index":0,"name":"200px-Feels_good_man.jpg","size":13768},
    {"index":1,"name":"kek/1.txt","size":22},
    {"index":2,"name":"videoplayback.mp4","size":188235949}
  ],
  "peers": [
    {
      "addr": "127.0.0.1:17555",
      "id": "bec28d6ff140884d7304b2698630cf84b9b4d14f1c55b3b504205bebf1c37133",
      "category": "seed",
      "upload_speed": 0,
      "download_speed": 0
    }
  ]
}
```

`bag_size` includes the header: `bag_size = size + header_size`.
`bag_pieces_num` is the number of pieces covering that size. `has_pieces_mask` is
a base64-encoded byte array; piece `i` uses bit `i % 8` of byte `i / 8`, with the
least significant bit first. It is `null` when torrent info is unavailable.
`merkle_hash` and peer IDs are hexadecimal. Peer `category` is `unknown`, `seed`,
or `leech`, based on advertised piece availability. Files and peers are empty
arrays when no entries are available.

An unknown bag returns HTTP 404 with `{"ok":false}`.

#### POST /api/v1/create

Create a bag synchronously from a local file or directory and enable seeding.
Relative paths are resolved from the node's working directory; an omitted or
empty `path` uses that directory. `description` is optional.
Optional `keep_only_paths` selects files by their
exact absolute paths and sets their order in the bag. For a directory, list the
individual file paths, rather than directory prefixes. Omitting or passing an
empty array includes all detected files. Selecting no matching files returns
HTTP 500.

Request:

```json
{
  "description": "Some Stuff",
  "path": "/root/some-dir",
  "keep_only_paths": [
    "/root/some-dir/first.txt",
    "/root/some-dir/second.txt"
  ]
}
```

Response, HTTP 200:

```json
{"bag_id":"6d791040957b5efa0311ef14f4278d92143b4c8369ad55d969ae6c1a6840ade8"}
```

Filesystem, creation, or persistence failures return HTTP 500 with `{"error":"..."}`.

#### POST /api/v1/remove

Remove the bag metadata and generated piece cache. `with_files` defaults to
`false`; set it to `true` to also remove content files. `--no-remove` protects
content files even when `with_files=true`. An unknown bag returns HTTP 404 with
`{"ok":false}`.

Request:

```json
{
  "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
  "with_files": false
}
```

Response, HTTP 200:

```json
{"ok":true}
```

#### POST /api/v1/stop

Stop downloading and uploading, and persist the stopped state. Resume with
`/add`. An unknown bag returns HTTP 404 with `{"ok":false}`.

Request:

```json
{"bag_id":"85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f"}
```

Response, HTTP 200:

```json
{"ok":true}
```

#### GET /api/v1/piece/proof?bag_id=<id>&piece=<index>

`piece` is a decimal unsigned 32-bit index starting at zero. The server reads and
verifies the requested piece before returning its Merkle proof. The proof is a
base64-encoded Bag of Cells (BOC).

Response, HTTP 200 (the proof value is shown as a placeholder):

```json
{"proof":"<base64-encoded-BOC>"}
```

Invalid ID or index syntax returns HTTP 400. An unknown bag returns HTTP 404
with `{"ok":false}`. Unavailable metadata, a missing/corrupt piece, or an index
outside the bag returns HTTP 404 with `{"ok":false,"error":"..."}`.

#### POST /api/v1/verify

Verify local data synchronously. With `only_files_existence=false` (the default),
verify recorded downloaded pieces for all files or the current file selection.
With `true`, check file existence without checking size or hashes.

Request:

```json
{
  "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
  "only_files_existence": false
}
```

Response, HTTP 200:

```json
{"ok":true}
```

`ok=true` means no problem was found in the checked data; `false` means a problem
was found. Metadata that has not loaded, an empty selection, or unrecorded pieces
can result in no content being checked. This result is not a download-completion
indicator.

Active downloaded bags resume their download planner after verification.
Stopped bags stay stopped; bags created locally do not start downloading.
Verification can invalidate pieces and remove damaged downloaded content files;
`--no-remove` protects content files. Locally created source files are preserved.

An unknown bag returns HTTP 400 with `{"error":"Bag is unknown"}`. Verification
errors return HTTP 500. Other HTTP methods return HTTP 405 with
`{"error":"Method not allowed"}`.

#### POST /api/v1/sign/provider

Sign the storage provider's ADNL ownership proof using this node's ADNL key.
`provider_id` is a 32-byte provider ID encoded as 64 hexadecimal characters.

Request:

```json
{"provider_id":"bec28d6ff140884d7304b2698630cf84b9b4d14f1c55b3b504205bebf1c37133"}
```

Response, HTTP 200 (binary values are shown as placeholders):

```json
{"key":"<base64-public-key>","signature":"<base64-signature>"}
```

`key` is the node's 32-byte Ed25519 public key; `signature` is the 64-byte signature
of the boxed TL `transport.ADNLProofScheme` containing the decoded provider ID.
Both fields are base64-encoded.

#### POST /api/v1/logger

Change runtime logging verbosity. The setting is not saved to configuration.

Request:

```json
{"verbosity":2}
```

Response, HTTP 200:

```json
{"ok":true}
```

Values up to 1 select error level, 2 selects info, and 3 or higher selects debug.
Level 11 also enables storage/provider loggers, 12 adds RLDP/BBR, and 13 adds
ADNL/DHT. Values above 13 are clamped to 13.

## License

[MIT](LICENSE).

<!-- Badges -->
[ton-svg]: https://img.shields.io/badge/Based%20on-TON-blue
[join-svg]: https://img.shields.io/badge/Join%20-Telegram-blue
[ton]: https://ton.org
[tg]: https://t.me/tonrh
