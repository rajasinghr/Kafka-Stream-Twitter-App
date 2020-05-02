package org.github.rajasinghr.kafkastreams;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;

public class TwitterAnalysisApp {
    public static void main(String[] args) {

        //Properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "<server>");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder builder  = new StreamsBuilder();

        //Input topic stream
        KStream<String, String> tweetInput = builder.stream("<input twitter feed topic>", Consumed.with(Serdes.String(), Serdes.String()));


//        KStream<String, String> data = tweetInput.peek((key,value) -> System.out.println(new JsonParser().parse(value.toString())
//                .getAsJsonObject().get("user").getAsJsonObject().get("screen_name").getAsString()));


        KTable<String, Long> tweetuserCount = tweetInput.mapValues((key, value) -> (new JsonParser().parse(value.toLowerCase())
                //Getting "Screen name" from tweet
                .getAsJsonObject().get("user").getAsJsonObject().get("screen_name").getAsString()))
                //.peek((key,value) -> System.out.println("key1="+key+", value="+value))
                //Selecting the name as Key
                .selectKey((ignoredKey, value) -> value)
                //.peek((key,value) -> System.out.println("key2="+key+", value="+value))
                //Group by key
                .groupByKey()
                //Count
                .count();

        //Pushing to output topic
        tweetuserCount.toStream().to( "stream output topic", Produced.with(Serdes.String(),Serdes.Long()));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        System.out.println(streams.toString());
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
