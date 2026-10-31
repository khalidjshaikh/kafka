# Kafka Random Numbers

A small Java program that:

1. Publishes a random number between **1 and 10** to a Kafka topic once per second.
2. Subscribes to that same topic.
3. Prints every number it receives to **stdout**.

## Layout

| Path | Purpose |
| --- | --- |
| `src/main/java/com/artificialintelligence/kafka/RandomNumberApp.java` | Entry point; wires up topic creation, consumer thread, producer thread, shutdown hook |
| `src/main/java/com/artificialintelligence/kafka/RandomNumberProducer.java` | Sends one random number per second |
| `src/main/java/com/artificialintelligence/kafka/RandomNumberConsumer.java` | Subscribes and prints received numbers |
| `src/main/resources/simplelogger.properties` | Keeps Kafka client logs (stderr) quiet |

## Prerequisites

- Java 17+ (built and tested with Java 27)
- Maven
- A Kafka broker listening on `localhost:9092` (e.g. Homebrew Kafka:
  `kafka-server-start /opt/homebrew/etc/kafka/server.properties`, or `brew services start kafka`)

## Build

```sh
mvn package
```

This produces a runnable uber-jar at `target/kafka-random-numbers-1.0.0.jar`.

## Run

The simplest way is the `run.sh` script, which builds if sources changed, starts a
local broker if it is not already running, and then launches the app:

```sh
./run.sh
```

Or run the jar directly (build first with `mvn package`):

```sh
java -jar target/kafka-random-numbers-1.0.0.jar
```

Stop with `Ctrl+C`.

## Scripts

| Script | Purpose |
| --- | --- |
| `./run.sh` | Build if needed, ensure a broker is reachable (starting the local one if necessary), run the app |
| `./kafka.sh start` | Start the local Homebrew Kafka broker and wait until it is *ready* |
| `./kafka.sh stop` | Stop the local broker and wait for the port to close |
| `./kafka.sh status` | Print whether the broker is reachable/ready (exit code 1 if not) |
| `./kafka.sh wait` | Block until the broker is ready |

`run.sh` options: `--no-build` (skip the stale-source check), `--no-start-kafka`
(fail if the broker is down instead of starting it), `-h`. Positional arguments and
the `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_TOPIC` environment variables work the same as
when launching the jar directly:

```sh
./run.sh                                          # localhost:9092, topic random-numbers
./run.sh --no-start-kafka mybroker:9092 my-topic  # remote broker, no auto-start
```

Notes:

- `kafka.sh start` probes the broker's metadata service, not just the TCP port,
  because Kafka opens the port before it is actually ready to serve clients.
- The broker config path defaults to `/opt/homebrew/etc/kafka/server.properties`;
  override it with `KAFKA_SERVER_CONFIG`.

### Configuration

| Settings | Default | Override |
| --- | --- | --- |
| Broker | `localhost:9092` | arg 1 or `KAFKA_BOOTSTRAP_SERVERS` |
| Topic | `random-numbers` | arg 2 or `KAFKA_TOPIC` |
| Consumer group | `random-number-printer` | — |

```sh
java -jar target/kafka-random-numbers-1.0.0.jar mybroker:9092 my-topic
```

The topic is created automatically (1 partition, replication factor 1) if it does not exist.
The consumer starts from the earliest offset the first time the group runs; on later runs it
continues from the last committed offset, so only new numbers are printed.

## Sample output

```
Kafka: localhost:9092 | topic: random-numbers | publishing a random number every 1000 ms
Subscribed to random-numbers, waiting for numbers...
Received random number: 2 (partition=0, offset=174)
Received random number: 10 (partition=0, offset=175)
Received random number: 4 (partition=0, offset=176)
Received random number: 9 (partition=0, offset=177)
Received random number: 5 (partition=0, offset=178)
Received random number: 7 (partition=0, offset=179)
Received random number: 2 (partition=0, offset=180)
Received random number: 5 (partition=0, offset=181)
Received random number: 8 (partition=0, offset=182)
Received random number: 5 (partition=0, offset=183)
Received random number: 2 (partition=0, offset=184)
Shutting down...
```
