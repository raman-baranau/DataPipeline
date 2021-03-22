package by.nn.proc.driver;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class JoltTransformTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private Serde<String> stringSerde = new Serdes.StringSerde();

    @Before
    public void prepare() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source =  builder.stream("input-topic");
        KStream<String, String> modified = source.mapValues(new JoltTransform.JoltMapper());
        modified.to("result-topic");
        Topology topology = builder.build();

        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "joltTransform");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), stringSerde.deserializer());
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void isJoltTransformed() {
        inputTopic.pipeInput("{\"name\":\"Vitaut\"}");
        assertThat(outputTopic.readValue(), equalTo("{\"name\":\"Vitaut\",\"excess\":\"default\"}"));
    }
}
