package com.artificialintelligence.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** Subscribes to the random-number topic and prints every received number to stdout. */
public final class RandomNumberConsumer implements Runnable {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final AtomicBoolean running;

    private volatile KafkaConsumer<String, String> consumer;

    public RandomNumberConsumer(String bootstrapServers, String topic, String groupId, AtomicBoolean running) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.running = running;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "random-number-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(props)) {
            this.consumer = c;
            c.subscribe(List.of(topic));
            System.out.println("Subscribed to " + topic + ", waiting for numbers...");

            while (running.get()) {
                ConsumerRecords<String, String> records = c.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received random number: %s (partition=%d, offset=%d)%n",
                            record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e; // unexpected unless a shutdown is in progress
            }
        } finally {
            consumer = null;
        }
    }

    /** Unblocks an in-progress poll so shutdown can complete promptly. Safe to call from any thread. */
    public void wakeup() {
        KafkaConsumer<String, String> c = consumer;
        if (c != null) {
            c.wakeup();
        }
    }
}
