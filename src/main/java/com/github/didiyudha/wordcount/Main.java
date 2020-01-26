package com.github.didiyudha.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        // 1. Stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // 2. Map values to lower case.
        KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase()).
                        // 3. Flat map values split by space
                        flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split(" "))).
                        // 4. Select key to apply a key (we discard the old key)
                        selectKey((ignoredKey, word) -> word).
                        // 5. Group by key before aggregations
                        groupByKey().
                       // 6. Count occurrences
                        count("Counts");
        wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // Printed the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the stream application.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
