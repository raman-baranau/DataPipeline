package by.nn.proc.driver;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class JoltTransform {

    private static class JoltMapper implements ValueMapper<String, String> {

        private String transform(String input) {
            Chainr chainr = Chainr.fromSpec(JsonUtils.classpathToList("/spec.json"));
            Object inputJson = JsonUtils.jsonToObject(input);
            Object output = chainr.transform(inputJson);
            return JsonUtils.toJsonString(output);
        }

        @Override
        public String apply(String s) {
            return transform(s);
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-jolttransform");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.119.253.38:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://10.119.244.61:8081");

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("in-jdbcavrotable");
        KStream<String, String> modified = source.mapValues(new JoltMapper());
        modified.to("kcbq-topic");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
