# TiREST

TiREST is a HTTP server for TiKV, providing curl'able user facing API (HTTP+JSON).

## Status

TiREST is already usable though still experimental.

The HTTP API is still in flux and may change in the near future.

## Feature

- [x] GET CAS LIST Health
- [x] CAS Command log to Kafka
- [x] UnsafePut UnsafeDelete BatchPut BatchDelete

## Install

```
$ make app
```

## Run

```
$ ./bin/tikv-proxy server --config=example/server.toml
```


## Test

```
$ make test
```

## API

### Start

```
$ ./bin/tikv-proxy server --config=example/example.toml
```

### Health

URI: `/api/v1/health`.

```
curl http://127.0.0.1:6100/api/v1/health -v
```

```
> GET /api/v1/health HTTP/1.1
> User-Agent: curl/7.29.0
> Host: 127.0.0.1:6100
> Accept: */*
>
< HTTP/1.1 204 No Content
< Date: Tue, 08 Sep 2020 07:07:54 GMT
<
```

### CAS

URI: `/api/v1/meta/{key}`.  

- `key` need url_base64
- `old`: old value 
- `new`: new value

```
curl http://127.0.0.1:6100/api/v1/meta/MTEx -d '{"new": "123"}' -v
```

or 

```
curl http://127.0.0.1:6100/api/v1/meta/MTEx -d '{"new": "123", "old": ""}' -v
```

```
> POST /api/v1/meta/MTEx HTTP/1.1
> Host: 127.0.0.1:6100
> User-Agent: curl/7.64.1
> Accept: */*
> Content-Length: 14
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 14 out of 14 bytes
< HTTP/1.1 204 No Content
< Date: Tue, 08 Sep 2020 06:18:09 GMT
```

### Get

URI: `/api/v1/meta/{key}`.

- `key` need url_base64

```
curl http://127.0.0.1:6100/api/v1/meta/MTEx  -v
```

```
> GET /api/v1/meta/MTEx HTTP/1.1
> User-Agent: curl/7.29.0
> Host: 127.0.0.1:6100
> Accept: */*
>
< HTTP/1.1 200 OK
< Content-Length: 3
< Content-Type: application/octet-stream
< Date: Tue, 08 Sep 2020 06:19:58 GMT
<
* Connection #0 to host 127.0.0.1 left intact
123#
```

### Store Get

```
./bin/tikv-proxy store --config=example/example.toml get MTEx
```

or

```
./bin/tikv-proxy store --config=example/server.toml -raw get 111
```

Output:
```
Value: 123
```

### CAS Again

- CAS OK

```
curl http://127.0.0.1:6100/api/v1/meta/MTEx -d '{"new": "234", "old": "123"}' -v
```

```
> POST /api/v1/meta/MTEx HTTP/1.1
> User-Agent: curl/7.29.0
> Host: 127.0.0.1:6100
> Accept: */*
> Content-Length: 28
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 28 out of 28 bytes
< HTTP/1.1 204 No Content
< Date: Tue, 08 Sep 2020 06:32:25 GMT
```

- CAS Failed

```
curl http://127.0.0.1:6100/api/v1/meta/MTEx -d '{"new": "456", "old": ""}' -v
```

```
> POST /api/v1/meta/MTEx HTTP/1.1
> User-Agent: curl/7.29.0
> Host: 127.0.0.1:6100
> Accept: */*
> Content-Length: 25
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 25 out of 25 bytes
< HTTP/1.1 409 Conflict
< Content-Type: application/json; charset=utf-8
< Date: Tue, 08 Sep 2020 06:32:59 GMT
< Content-Length: 32
<
* Connection #0 to host 127.0.0.1 left intact
{"error":"check and set failed"}
```


### LIST

URL: `/api/v1/List/`.

```
curl http://127.0.0.1:6100/api/v1/list/ -H "X-Start: 1" -H "X-End: 2" -H "X-Limit: 1" -H "X-Raw: true" -v
```

```
> GET /api/v1/list/ HTTP/1.1
> User-Agent: curl/7.29.0
> Host: 127.0.0.1:6100
> Accept: */*
> X-Start: 1
> X-End: 2
> X-Limit: 1
> X-Raw: true
>
< HTTP/1.1 200 OK
< Content-Length: 29
< Content-Type: application/json
< Date: Tue, 08 Sep 2020 06:35:42 GMT
<
* Connection #0 to host 127.0.0.1 left intact
[{"key":"111","value":"234"}]#
```

### Store LIST

```
./bin/tikv-proxy store --config=example/server.toml -raw list --start=1 --end=2 --limit=2
```

```
Key: 111
Value: 234
Key: 123
Value: 456
```