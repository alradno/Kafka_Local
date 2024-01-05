package com.example.kafka_local;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Properties;

public class KafkaStreamsConfig {

    public Properties setProperties(String APPLICATION_ID, String BOOTSTRAP_SERVERS) throws IOException {
        //Configuración Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }

    public Topology setTopology(String SOURCE_TOPIC, RestHighLevelClient client, String indexName) {
        Topology topology = new Topology();
        topology.addSource("source", SOURCE_TOPIC)
                .addProcessor("custom-processor", CustomProcessor::new, "source")
                .addProcessor("elastic-processor", () -> new ElasticSearchProcessor(client, indexName), "custom-processor")
                .addSink("elastic-sink", "elastic-search-index", "elastic-processor");
        return topology;
    }
}