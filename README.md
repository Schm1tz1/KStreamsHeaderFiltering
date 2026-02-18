# KStreams Header Filtering App

A Kafka Streams application that filters Kafka record headers using configurable regular expressions. It reads records from an input topic, strips unwanted headers based on a regex pattern, and forwards the cleaned records to an output topic.

## Processing Logic

The core component is `HeaderFilterProcessor`, a Kafka Streams Processor API processor that:

1. Receives each record from the input topic
2. Evaluates every header key against a configured regular expression
3. Keeps only headers whose keys match the pattern (or don't match, when negation is enabled)
4. Forwards the record with the filtered set of headers to the output topic

Example use cases:
- Strip vendor/tracing headers (e.g. `x-datadog-*`, `azure-event-hub`) before publishing to downstream consumers
- Allowlist only a known set of headers using a positive regex match
- Remove headers by pattern using negated regex (`streamsApp.negateRegExp=true`)

## Data Format / (De-)Serialization

- Keys and values are treated as plain `String` (UTF-8)
- Headers are filtered by key name using Java regex; header values are passed through unchanged
- Serdes: `Serdes.String()` for both key and value (default, configurable via standard Kafka Streams properties)

## Build

```bash
# Compile
mvn compile

# Run tests
mvn test

# Package to jar
mvn package
```

Requires Java 17 and Maven.

## Testing

Unit tests and Kafka Streams Topology Test Driver (TTD) integration tests are included:

- `HeaderFilterProcessorTest` — unit tests for the filter processor with various regex and predicate combinations
- `StreamsPipelineTest` — end-to-end topology test using `TopologyTestDriver`

Run tests separately:

```bash
mvn test
```

## Application Configuration

The application reads one or two `.properties` files at startup. Mandatory pipeline properties go in the main config file; optional overrides can be supplied via a second file.

### Required properties

| Property | Description |
|---|---|
| `streamsApp.inputTopic` | Kafka topic to read from |
| `streamsApp.outputTopic` | Kafka topic to write to |
| `streamsApp.regExpString` | Java regex applied to each header key |

### Optional properties

| Property | Default | Description |
|---|---|---|
| `streamsApp.negateRegExp` | `false` | When `true`, headers that match the regex are **removed** instead of kept |
| `bootstrap.servers` | `localhost:9092` | Kafka broker(s) |

### Example: strip Azure and Datadog headers

```properties
bootstrap.servers=localhost:9092
streamsApp.inputTopic=raw-events
streamsApp.outputTopic=clean-events
streamsApp.regExpString=^(azure-.*|x-datadog-.*|x-opt-.*).*
streamsApp.negateRegExp=true
```

Standard Kafka Streams and Kafka client properties (security, schema registry, etc.) can be added to the same file.

## Deployment, Running

### Command line

```bash
java -jar target/KStreamsTemplate-0.1.jar -c /path/to/kstreams.properties
```

Optional flags:

| Flag | Description |
|---|---|
| `-c`, `--config-file` | Primary configuration file |
| `-C`, `--additional-config-file` | Secondary/override configuration file |
| `--enable-monitoring-interceptor` | Enable Confluent Monitoring Interceptors (for Confluent Control Center) |

### Docker

A Docker image can be built using the Dockerfile in `src/main/docker/` (via the `docker-maven-plugin`):

```bash
mvn docker:build
docker run -v /path/to/kstreams.properties:/app/config/kstreams.properties schmitzi/kstreams-template:0.1
```

The container uses Alpine Linux with a headless JRE. The config file is mounted at `/app/config/kstreams.properties`.

## Log Format

Logging is via SLF4J backed by Log4j 2, defaulting to INFO level on STDOUT.

Override the log configuration at runtime:

```bash
java -Dlog4j2.configurationFile=/path/to/log4j2.properties -jar target/KStreamsTemplate-0.1.jar -c kstreams.properties
```

## Metrics

The application exposes JMX metrics for Kafka Streams via the [JMX Prometheus Java Agent](https://github.com/prometheus/jmx_exporter) on port **1234** (when running via Docker/the startup script).

The JMX exporter configuration in `src/main/docker/config/jmx_exporter_kafka_streams.yml` scrapes:
- `kafka.streams:*` — Kafka Streams thread, task, processor, and state store metrics
- `kafka.consumer:*` / `kafka.producer:*` — underlying client metrics (optional, increases scrape duration)

Custom sensors exposed by `HeaderFilterProcessor` per application instance:

| Sensor | Description |
|---|---|
| `processor-in` | Rate/total of records received |
| `processor-out` | Rate/total of records forwarded |
| `processor-skipped` | Rate/total of records forwarded unmodified due to processing errors |
| `input-header-count` | Rate/total of header entries seen on input |
| `input-header-bytes` | Rate/total of header bytes seen on input |
| `output-header-count` | Rate/total of header entries forwarded |
| `output-header-bytes` | Rate/total of header bytes forwarded |
