# mini-tsdb

**mini-tsdb** is a minimal time-series database written in Go with support for Prometheus `remote_write` and `remote_read` APIs. This project is designed as an educational playground to explore how time-series storage engines work under the hood.

## Features
- **Remote Write**: Accepts time-series data from Prometheus
- **Remote Read**: Responds to Prometheus read queries
- **In-Memory Storage**: Keeps data in memory and uses inverted index for quick reads
- **Write-Ahead Log (WAL)**: Appends all incoming data to disk safely
- **Replay on Startup**: WAL is replayed to restore in-memory state

## TODO
- [X] Implement `remote_write` API
- [X] Implement `remote_read` API
- [X] Implement in-memory storage with inverted index
- [X] Implement Write-Ahead Log for durability
- [ ] Implement XOR compression for float64 values
- [ ] Implement compaction logic to deduplicate and rewrite WAL segments
- [ ] Multi instance support

## Local run

1. Run docker compose: it will start a mini-tsdb instance, prometheus, grafana and a sample app to get metrics from.
   ```bash
   docker compose up -d
   ```
2. Do a few requests to the sample app to generate some metrics:
   ```bash
   curl http://localhost:8080/metrics
   ```
3. Request metrics from Prometheus:
    ```bash
    curl -s http://localhost:9090/api/v1/query?query=http_requests_total | json_pp
{
   "data" : {
      "result" : [
         {
            "metric" : {
               "__name__" : "http_requests_total",
               "handler" : "/",
               "instance" : "host.docker.internal:8080",
               "job" : "testapp",
               "method" : "GET"
            },
            "value" : [
               1751220927.244,
               "4"
            ]
         }
      ],
      "resultType" : "vector"
   },
   "status" : "success"
}
    ```

