package milltanya.highload.suspiciousbetbybettor.configuration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Score;

import java.time.Duration;

import static milltanya.highload.suspiciousbetbybettor.configuration.KafkaConfiguration.GOAL_TOPIC;
import static milltanya.highload.suspiciousbetbybettor.configuration.KafkaConfiguration.SUSPICIOUS_BET_BY_BETTOR;
import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;

@Configuration
public class TopologyConfiguration {

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, Bet> bets = streamsBuilder.stream(
                BET_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class)));

        KStream<String, EventScore> scores = streamsBuilder.stream(
                EVENT_SCORE_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(EventScore.class)));

        KStream<String, Long> goals = scores
                .filter((k, v) -> v.getScore().getHome() + v.getScore().getAway() >= 1)
                .groupByKey()
                .aggregate(
                        () -> new GoalInfo(new Score(), "", 0L),
                        (k, v, a) -> new GoalInfo(
                                v.getScore(),
                                (new Score(
                                        v.getScore().getHome() - a.getCurrentScore().getHome(),
                                        v.getScore().getAway() - a.getCurrentScore().getAway()
                                )).toString().equals("1:0") ? "H" : "A",
                                v.getTimestamp()),
                        Materialized.with(Serdes.String(), new JsonSerde<>(GoalInfo.class)))
                .toStream()
                .map((k, v) -> KeyValue.pair(k + ":" + v.getScorerTeam(), v.getTimestamp()));

        goals.to(GOAL_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KStream<String, Long> goalsInput = streamsBuilder.stream(GOAL_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()));

        KStream<String, Bet> suspiciousBets = bets.join(
                goalsInput,
                SuspiciousBet::new,
                JoinWindows.of(Duration.ofSeconds(1)),
                StreamJoined.with(Serdes.String(), new JsonSerde<>(Bet.class), Serdes.Long()))
                .filter((k, v) -> v.getBet().getTimestamp() < v.getGoalTimestamp())
                .map((k, v) -> KeyValue.pair(v.getBet().getBettor(), v.getBet()));

        suspiciousBets.to(SUSPICIOUS_BET_BY_BETTOR, Produced.with(Serdes.String(), new JsonSerde<>(Bet.class)));

        Topology topology = streamsBuilder.build();
        System.out.println("========================================");
        System.out.println(topology.describe());
        System.out.println("========================================");
        // https://zz85.github.io/kafka-streams-viz/
        return topology;
    }
}
