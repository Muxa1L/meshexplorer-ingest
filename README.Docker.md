# Docker Setup for MeshExplorer Ingester

This document explains how to build and run the ingester.py application using Docker.

## Prerequisites

- Docker installed on your system
- Docker Compose (optional, but recommended)

## Building the Docker Image

```bash
docker build -t meshexplorer-ingester .
```

## Running with Docker Run

```bash
docker run -d \
  --name meshexplorer-ingester \
  -e CLICKHOUSE_HOST=your_clickhouse_host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_USER=your_user \
  -e CLICKHOUSE_PASS=your_password \
  -e MQTT_HOST=your_mqtt_host \
  -e MQTT_PORT=1883 \
  -e MQTT_USER=meshcore \
  -e MQTT_PASS=meshcore \
  -e MQTT_TOPIC=meshcore/KRR \
  meshexplorer-ingester
```

## Running with Docker Compose (Recommended)

1. Create a `.env` file in the project root with your configuration:

```env
CLICKHOUSE_HOST=your_clickhouse_host
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=your_user
CLICKHOUSE_PASS=your_password

MQTT_HOST=your_mqtt_host
MQTT_PORT=1883
MQTT_USER=meshcore
MQTT_PASS=meshcore
MQTT_TOPIC=meshcore/KRR
```

2. Start the service:

```bash
docker-compose up -d
```

3. View logs:

```bash
docker-compose logs -f ingester
```

4. Stop the service:

```bash
docker-compose down
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_HOST` | ClickHouse server hostname | `localhost` |
| `CLICKHOUSE_PORT` | ClickHouse server port | `8123` |
| `CLICKHOUSE_USER` | ClickHouse username | `default` |
| `CLICKHOUSE_PASS` | ClickHouse password | (empty) |
| `MQTT_HOST` | MQTT broker hostname | `localhost` |
| `MQTT_PORT` | MQTT broker port | `1883` |
| `MQTT_USER` | MQTT username | `meshcore` |
| `MQTT_PASS` | MQTT password | `meshcore` |
| `MQTT_TOPIC` | MQTT topic | `meshcore/KRR` |

## Troubleshooting

### Connection Issues

If the container cannot connect to ClickHouse or MQTT:

- For local services, use `host.docker.internal` instead of `localhost` on Windows/Mac
- On Linux, use `--network host` mode or ensure services are accessible from the Docker network
- Uncomment `network_mode: host` in docker-compose.yml for local development

### View Container Logs

```bash
docker logs -f meshexplorer-ingester
```

### Rebuild After Changes

```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```
