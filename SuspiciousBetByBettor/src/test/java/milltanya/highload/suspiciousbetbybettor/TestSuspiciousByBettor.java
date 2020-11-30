package milltanya.highload.suspiciousbetbybettor;

import milltanya.highload.suspiciousbetbybettor.configuration.KafkaConfiguration;
import milltanya.highload.suspiciousbetbybettor.configuration.TopologyConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Outcome;
import ru.curs.counting.model.Score;

import static milltanya.highload.suspiciousbetbybettor.configuration.KafkaConfiguration.GOAL_TOPIC;
import static milltanya.highload.suspiciousbetbybettor.configuration.KafkaConfiguration.SUSPICIOUS_BET_BY_BETTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;

public class TestSuspiciousByBettor {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Bet> betTopic;
    private TestInputTopic<String, EventScore> eventScoreTopic;
    private TestOutputTopic<String, Long> middleTopic;
    private TestOutputTopic<String, Bet> outputTopic;

    @BeforeEach
    public void setUp() {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(
                topology, config.asProperties());
        betTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(Bet.class).serializer());
        eventScoreTopic = topologyTestDriver.createInputTopic(EVENT_SCORE_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(EventScore.class).serializer());
        middleTopic =
                topologyTestDriver.createOutputTopic(GOAL_TOPIC, Serdes.String().deserializer(),
                        Serdes.Long().deserializer());
        outputTopic =
                topologyTestDriver.createOutputTopic(SUSPICIOUS_BET_BY_BETTOR, Serdes.String().deserializer(),
                        new JsonSerde<>(Bet.class).deserializer());
    }

    @AfterEach
    public void cleanUp() {
        topologyTestDriver.close();
    }

    @Test
    void testTopology() {
        Bet bet = Bet.builder()
                .bettor("John Doe")
                .match("Germany-Belgium")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.7)
                .timestamp(1604364399109L)
                .build();
        EventScore eventScore0 = new EventScore(
                "Germany-Belgium",
                new Score(1, 1),
                1604364323464L);
        EventScore eventScore1 = new EventScore(
                "Germany-Belgium",
                new Score(2, 1),
                1604364399822L);
        EventScore eventScore2 = new EventScore(
                "Germany-Belgium",
                new Score(2, 2),
                1604364476585L);

        eventScoreTopic.pipeInput(eventScore0.getEvent(), eventScore0, eventScore0.getTimestamp());
        betTopic.pipeInput(bet.key(), bet, bet.getTimestamp());
        eventScoreTopic.pipeInput(eventScore1.getEvent(), eventScore1, eventScore1.getTimestamp());

        TestRecord<String, Long> goalRecord = middleTopic.readRecord();
        assertEquals(eventScore0.getEvent() + ":D", goalRecord.key());
        assertEquals(eventScore0.getTimestamp(), goalRecord.getValue());

        goalRecord = middleTopic.readRecord();
        assertEquals(eventScore1.getEvent() + ":H", goalRecord.key());
        assertEquals(eventScore1.getTimestamp(), goalRecord.getValue());

        TestRecord<String, Bet> suspiciousBetRecord = outputTopic.readRecord();
        assertEquals(bet.getBettor(), suspiciousBetRecord.key());
        assertEquals(bet, suspiciousBetRecord.getValue());

        eventScoreTopic.pipeInput(eventScore2.getEvent(), eventScore2, eventScore2.getTimestamp());

        goalRecord = middleTopic.readRecord();
        assertEquals(eventScore2.getEvent() + ":A", goalRecord.key());
        assertEquals(eventScore2.getTimestamp(), goalRecord.getValue());

        assertTrue(outputTopic.isEmpty());
    }

    void placeBet(Bet bet) {
        betTopic.pipeInput(bet.key(), bet, bet.getTimestamp());
    }
    void placeEvent(EventScore event) {
        eventScoreTopic.pipeInput(event.getEvent(), event, event.getTimestamp());
    }
    @Test
    void testFraud() {
        long currentTimestamp = System.currentTimeMillis();
        Score score = new Score().goalHome();
        placeEvent(new EventScore("A-B", score, currentTimestamp));
        score = score.goalHome();
        placeEvent(new EventScore("A-B", score, currentTimestamp + 100 * 1000));
        score = score.goalAway();
        placeEvent(new EventScore("A-B", score, currentTimestamp + 200 * 1000));
        //ok
        placeBet(new Bet("John", "A-B", Outcome.H, 1, 1, currentTimestamp - 2000));
        //ok
        placeBet(new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp + 100 * 1000 - 2000));
        //fraud?
        Bet fraud1 = new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp + 100 * 1000 - 10);
        placeBet(fraud1);
        //fraud?
        Bet fraud2 = new Bet("Mary", "A-B", Outcome.A, 1, 1, currentTimestamp + 200 * 1000 - 20);
        placeBet(fraud2);

        TestRecord<String, Bet> suspiciousBetRecord = outputTopic.readRecord();
        assertEquals(fraud1.getBettor(), suspiciousBetRecord.key());
        assertEquals(fraud1, suspiciousBetRecord.getValue());

        suspiciousBetRecord = outputTopic.readRecord();
        assertEquals(fraud2.getBettor(), suspiciousBetRecord.key());
        assertEquals(fraud2, suspiciousBetRecord.getValue());

        assertTrue(outputTopic.isEmpty());
    }
}
