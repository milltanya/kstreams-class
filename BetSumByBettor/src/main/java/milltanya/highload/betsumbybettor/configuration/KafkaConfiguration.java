package milltanya.highload.betsumbybettor.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    public static final String BET_BY_BETTOR_TOPIC = "bet_by_bettor";
    public static final String BET_SUM_BY_BETTOR_TOPIC = "bet_sum_by_bettor";

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration getStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "milltanya-bet-sum-by-bettor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public NewTopic betByBettorTopic() {
        return TopicBuilder
                .name(BET_BY_BETTOR_TOPIC)
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic betSumByBettorTopic() {
        return TopicBuilder
                .name(BET_SUM_BY_BETTOR_TOPIC)
                .partitions(10)
                .replicas(1)
                .compact()
                .build();
    }

}
