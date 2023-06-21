# Tonutils Storage
[![Based on TON][ton-svg]][ton] [![Join our group][join-svg]][tg]

Alternative TON Storage implementation based on [tonutils-go](https://github.com/xssnick/tonutils-go), with [HTTP API](#http-api).

You could freely use this storage in any type of projects.

If you want to support this project and help us to develop projects like this 100% free and open source, we would be grateful if you donate any amount of TON or Jettons to `EQBx6tZZWa2Tbv6BvgcvegoOQxkRrVaBVwBOoW85nbP37_Go` ☺️

<img width="1082" alt="Screen Shot 2023-06-12 at 20 22 54" src="https://github.com/xssnick/tonutils-storage/assets/9332353/c321230b-0a6c-462d-946d-66d31bdb588a">

## Quick start

1. Download precompiled version:
   * [Linux AMD64](https://github.com/xssnick/tonutils-storage/releases/download/v0.2.5/tonutils-storage-linux-amd64)
   * [Linux ARM64](https://github.com/xssnick/tonutils-storage/releases/download/v0.2.5/tonutils-storage-linux-arm64)
   * [Windows x64](https://github.com/xssnick/tonutils-storage/releases/download/v0.2.5/tonutils-storage-x64.exe)
   * [Mac Intel](https://github.com/xssnick/tonutils-storage/releases/download/v0.2.5/tonutils-storage-mac-amd64)
   * [Mac Apple Silicon](https://github.com/xssnick/tonutils-storage/releases/download/v0.2.5/tonutils-storage-mac-arm64)
2. Run
`./tonutils-storage`
3. Try `download 85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f`
4. Use `list` command to check progress

## CLI

At this moment 4 commands are available:

* Create bag: `create [path] [description]`
* Download bag: `download [bag_id]`
* List bags: `list`
* Display help: `help`

At the first start you will see something like `Using port checker tonutils.com at 31.172.68.159`. 
Storage will try to resolve your external ip address. In case if it fails, to seed bags you will need to manually specify ip in config.json inside db folder  .

### Minimum requirements

* RAM: **512 MB**
* CPU: **2 Cores**
* Enough disk space to host your files
* Internet connection

### HTTP API

When running with flag `--api ip:port`, you could access storage using HTTP API and control it.

If you want to enable HTTP Basic Auth you could use additional flags `--api-login [login] --api-password [password]`

Example: `./tonutils-storage --api 127.0.0.1:8192 --api-login admin --api-password 123456`

You could [download Postman collection](https://github.com/xssnick/tonutils-storage/blob/master/Tonutils%20Storage.postman_collection.json) or check examples below.

#### POST /api/v1/add

Download bag by id. If `download_all` is false and files are empty, only header will be downloaded.

After adding, you could call `GET /api/v1/details?bag_id=[id]`, when header is available you will see the list of files. Call `add` again with required files ids.

Request:
```json
{
    "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
    "path": "/root/downloads",
    "files": [0,1,2],
    "download_all": false
}
```
Response:
```json
{
    "ok": true
}
```

#### GET /api/v1/list

Response:
```json
{
  "bags": [
    {
      "bag_id": "6d791040957b5efa0311ef14f4278d92143b4c8369ad55d969ae6c1a6840ade8",
      "description": "Some Stuff",
      "downloaded": 150126947,
      "size": 150126947,
      "peers": 0,
      "download_speed": 0,
      "upload_speed": 0,
      "files_count": 17,
      "dir_name": "torrent/",
      "completed": true,
      "header_loaded": true,
      "info_loaded": true,
      "active": true,
      "seeding": true
    },
    {
      "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
      "description": "FunnyPack",
      "downloaded": 188249739,
      "size": 188249739,
      "peers": 0,
      "download_speed": 0,
      "upload_speed": 0,
      "files_count": 3,
      "dir_name": "video/",
      "completed": false,
      "header_loaded": true,
      "info_loaded": true,
      "active": false,
      "seeding": false
    }
  ]
}
```

* Size in bytes and speed in bytes per second

#### GET /api/v1/details?bag_id=[id]
Response:
```json
{
    "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
    "description": "FunnyPack",
    "downloaded": 130936,
    "size": 188249739,
    "download_speed": 0,
    "upload_speed": 0,
    "files_count": 3,
    "dir_name": "video/",
    "completed": false,
    "header_loaded": true,
    "info_loaded": true,
    "active": true,
    "seeding": true,
    "files": [
        {
            "index": 0,
            "name": "200px-Feels_good_man.jpg",
            "size": 13768
        },
        {
            "index": 1,
            "name": "kek/1.txt",
            "size": 22
        },
        {
            "index": 2,
            "name": "videoplayback.mp4",
            "size": 188235949
        }
    ],
    "peers": [
        {
            "addr": "31.172.68.159:17555",
            "id": "bec28d6ff140884d7304b2698630cf84b9b4d14f1c55b3b504205bebf1c37133",
            "upload_speed": 0,
            "download_speed": 0
        },
        {
            "addr": "185.18.52.220:17555",
            "id": "f546878e8e4bd3885cc623ab0440f05abb12138d4701cee998e4f073ec9ade7f",
            "upload_speed": 0,
            "download_speed": 0
        },
        {
            "addr": "185.195.69.60:13333",
            "id": "04e7276cc1d3d480c70c83b0fb66d88412e34a5734b15a412155b1e9b5ff17a7",
            "upload_speed": 0,
            "download_speed": 0
        }
    ]
}
```

#### POST /api/v1/create
Request:
```json
{
    "description": "Some Stuff",
    "path": "/Users/admin/some-dir"
}
```

Response:
```json
{
    "bag_id": "6d791040957b5efa0311ef14f4278d92143b4c8369ad55d969ae6c1a6840ade8"
}
```

#### POST /api/v1/remove
Request:
```json
{
   "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f",
   "with_files": false
}
```

Response:
```json
{
   "ok": true
}
```

#### POST /api/v1/stop
Request:
```json
{
   "bag_id": "85d0998dcf325b6fee4f529d4dcf66fb253fc39c59687c82a0ef7fc96fed4c9f"
}
```

Response:
```json
{
   "ok": true
}
```

##### GET /api/v1/piece/proof?bag_id=[bag_id]&piece=[piece_index]

Response:
```json
{
   "proof": "te6ccgECGAEAAhIACUYDHTW6QVztpEzC31WcBwM8yBMvSGXQd3MD+wAhTsHW0xIACwEiAAMCKEgBAc7bhbyT2s90nYhAtDWBlX9L8/vk1RRSnBoStVbcYP9yAAoiAAUEKEgBAU3BESCG4LO5vLWDqEeHHaVoNfsW4uB6ulW98Ig+0F14AAkiAAcGKEgBAUTJbcOij+kzzAfzGdAVE+dqUkVeEh8k0Fa4Er17mqv0AAgiAAkIKEgBAem7eQT+47rVFPOw2xTXStiPmmvp8qnTRoJU4ytEVjKBAAciAAsKKEgBARYLz8mwWc5C7/m2pxPyuvHyHCJxYYERZpIVLFOfeQ+QAAYiAA0MKEgBAWPbB3lTLu3TmzksGONbNpq0B9ZzIkIngurqakszU2VOAAUiAA8OKEgBAZ1yE9s5xTNpnnPznOY9ec7CoWth9ss7zx+BpPooewEeAAQiABEQKEgBAXZvDBuAa212EubKLPaSX62+k36vechsm9D50Qo2cTDiAAMiABMSKEgBAbhbC8Eeb4JXZSowetpDFDnu6kiV98PUmtc1LnTvY5JXAAIiABUUKEgBAUzzpEPNmZLfP8J+90j7cniw7f3eiKvvJWTGx3sX82yvAAECABcWAED6S6nWCQpkeziAkJwbLPvFX6hjRdFFPlXF6dQXqauQOwBAixDSw4tIVZgM4DvkN6Juu2Mu5FA3yuTtitTe60WOzVE="
}
```

<!-- Badges -->
[ton-svg]: https://img.shields.io/badge/Based%20on-TON-blue
[join-svg]: https://img.shields.io/badge/Join%20-Telegram-blue
[ton]: https://ton.org
[tg]: https://t.me/tonrh

