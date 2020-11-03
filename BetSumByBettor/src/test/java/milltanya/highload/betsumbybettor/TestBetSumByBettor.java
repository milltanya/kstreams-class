package milltanya.highload.betsumbybettor;

import milltanya.highload.betsumbybettor.configuration.KafkaConfiguration;
import milltanya.highload.betsumbybettor.configuration.TopologyConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.Outcome;

import static milltanya.highload.betsumbybettor.configuration.KafkaConfiguration.BET_BY_BETTOR_TOPIC;
import static milltanya.highload.betsumbybettor.configuration.KafkaConfiguration.BET_SUM_BY_BETTOR_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.curs.counting.model.TopicNames.BET_TOPIC;

public class TestBetSumByBettor {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Bet> inputTopic;
    private TestOutputTopic<String, Long> middleTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    public void setUp() {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(
                topology, config.asProperties());
        inputTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(Bet.class).serializer());
        middleTopic =
                topologyTestDriver.createOutputTopic(BET_BY_BETTOR_TOPIC, Serdes.String().deserializer(),
                        Serdes.Long().deserializer());
        outputTopic =
                topologyTestDriver.createOutputTopic(BET_SUM_BY_BETTOR_TOPIC, Serdes.String().deserializer(),
                        Serdes.Long().deserializer());
    }

    @AfterEach
    public void cleanUp() {
        topologyTestDriver.close();
    }

    @Test
    void testTopology() {
        Bet bet1 = Bet.builder()
                .bettor("John Doe")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.7).build();
        inputTopic.pipeInput(bet1.key(), bet1);

        TestRecord<String, Long> record = middleTopic.readRecord();
        assertEquals(bet1.getBettor(), record.key());
        assertEquals(bet1.getAmount(), record.getValue());

        record = outputTopic.readRecord();
        assertEquals(bet1.getBettor(), record.key());
        assertEquals(bet1.getAmount(), record.getValue());

        Bet bet2 = Bet.builder()
                .bettor("Ivan Ivanov")
                .match("Spain-France")
                .outcome(Outcome.A)
                .amount(80)
                .odds(1.3).build();
        inputTopic.pipeInput(bet2.key(), bet2);

        record = middleTopic.readRecord();
        assertEquals(bet2.getBettor(), record.key());
        assertEquals(bet2.getAmount(), record.getValue());

        record = outputTopic.readRecord();
        assertEquals(bet2.getBettor(), record.key());
        assertEquals(bet2.getAmount(), record.getValue());

        Bet bet3 = Bet.builder()
                .bettor("John Doe")
                .match("Belgium-Italy")
                .outcome(Outcome.D)
                .amount(30)
                .odds(1.9).build();
        inputTopic.pipeInput(bet3.key(), bet3);

        record = middleTopic.readRecord();
        assertEquals(bet3.getBettor(), record.key());
        assertEquals(bet3.getAmount(), record.getValue());

        record = outputTopic.readRecord();
        assertEquals(bet3.getBettor(), record.key());
        assertEquals(bet1.getAmount() + bet3.getAmount(), record.getValue());
    }
}
