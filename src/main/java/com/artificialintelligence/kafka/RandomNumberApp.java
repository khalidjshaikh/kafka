package com.artificialintelligence.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Publishes a random number between 1 and 10 to a Kafka topic once per second,
 * consumes the same topic, and prints every received number to stdout.
 *
 * <p>Usage: {@code RandomNumberApp [bootstrap-servers] [topic]}. Defaults to
 * {@code localhost:9092} and {@code random-numbers}; both can also be set via the
 * {@code KAFKA_BOOTSTRAP_SERVERS} and {@code KAFKA_TOPIC} environment variables.
 */
public final class RandomNumberApp {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "random-numbers";
    private static final String CONSUMER_GROUP = "random-number-printer";
    private static final long PUBLISH_INTERVAL_MS = 1_000;

    private RandomNumberApp() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = resolve(args, 0, "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = resolve(args, 1, "KAFKA_TOPIC", DEFAULT_TOPIC);

        System.out.printf("Kafka: %s | topic: %s | publishing a random number every %d ms%n",
                bootstrapServers, topic, PUBLISH_INTERVAL_MS);

        createTopicIfMissing(bootstrapServers, topic);

        AtomicBoolean running = new AtomicBoolean(true);

        RandomNumberConsumer consumer = new RandomNumberConsumer(bootstrapServers, topic, CONSUMER_GROUP, running);
        Thread consumerThread = new Thread(consumer, "random-number-consumer");
        consumerThread.start();

        // Give the consumer a moment to join the group before the first publish.
        Thread.sleep(1_000);

        Thread producerThread = new Thread(
                new RandomNumberProducer(bootstrapServers, topic, PUBLISH_INTERVAL_MS, running),
                "random-number-producer");
        producerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            running.set(false);
            consumer.wakeup();
            try {
                producerThread.join(5_000);
                consumerThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown"));

        producerThread.join();
        consumerThread.join();
    }

    private static String resolve(String[] args, int index, String envVar, String defaultValue) {
        if (args.length > index && !args[index].isBlank()) {
            return args[index];
        }
        return System.getenv().getOrDefault(envVar, defaultValue);
    }

    private static void createTopicIfMissing(String bootstrapServers, String topic) {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);

        try (Admin admin = Admin.create(config)) {
            if (!admin.listTopics().names().get().contains(topic)) {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
                System.out.println("Created topic " + topic);
            }
        } catch (Exception e) {
            System.err.printf("Could not reach Kafka at %s: %s%n", bootstrapServers, e.getMessage());
            System.err.println("Start a broker first, e.g.: kafka-server-start /opt/homebrew/etc/kafka/server.properties");
            System.exit(1);
        }
    }
}
