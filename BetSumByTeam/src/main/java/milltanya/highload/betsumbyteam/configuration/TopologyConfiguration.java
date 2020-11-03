package milltanya.highload.betsumbyteam.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.Outcome;

import static milltanya.highload.betsumbyteam.configuration.KafkaConfiguration.BET_BY_TEAM_TOPIC;
import static milltanya.highload.betsumbyteam.configuration.KafkaConfiguration.BET_SUM_BY_TEAM_TOPIC;
import static ru.curs.counting.model.TopicNames.*;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, Bet> input = streamsBuilder.stream(
                BET_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class)));
        KStream<String, Long> bets = input
                .filter((k, v) -> v.getOutcome() != Outcome.D)
                .map((k, v) -> KeyValue.pair(
                        v.getOutcome() == Outcome.H ?
                                v.getMatch().split("-")[0] :
                                v.getMatch().split("-")[1],
                        v.getAmount()));
        bets.to(BET_BY_TEAM_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KStream<String, Long> betsInput = streamsBuilder.stream(BET_BY_TEAM_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()));
        KTable<String, Long> totals = betsInput.groupByKey().reduce(Long::sum);

        totals.toStream().to(BET_SUM_BY_TEAM_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = streamsBuilder.build();
        System.out.println("========================================");
        System.out.println(topology.describe());
        System.out.println("========================================");
        // https://zz85.github.io/kafka-streams-viz/
        return topology;
    }
}
