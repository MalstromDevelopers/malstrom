# Running the examples

https://github.com/risingwavelabs/nexmark-bench

1. Start the docker stack

```bash
cd kafka-stack-docker-compose
podman compose -f zk-single-kafka-single.yml up
```

2. Setup topics

`nexmark-server -c`

1. Ingest data: nexmark-server --event-rate 100000 --max-events 100000 
