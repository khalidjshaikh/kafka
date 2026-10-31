package com.artificialintelligence.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/** Publishes a random number between 1 and 10 (inclusive) to a Kafka topic at a fixed interval. */
public final class RandomNumberProducer implements Runnable {

    static final int MIN = 1;
    static final int MAX = 10;

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final String bootstrapServers;
    private final String topic;
    private final long intervalMs;
    private final AtomicBoolean running;

    public RandomNumberProducer(String bootstrapServers, String topic, long intervalMs, AtomicBoolean running) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.intervalMs = intervalMs;
        this.running = running;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "random-number-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            while (running.get()) {
                int number = ThreadLocalRandom.current().nextInt(MIN, MAX + 1);
                String value = Integer.toString(number);

                producer.send(new ProducerRecord<>(topic, value), (metadata, error) -> {
                    if (error != null) {
                        System.err.println("Publish failed for " + value + ": " + error.getMessage());
                    }
                });

                Thread.sleep(intervalMs);
            }
            producer.flush();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            producer.close(CLOSE_TIMEOUT);
        }
    }
}
