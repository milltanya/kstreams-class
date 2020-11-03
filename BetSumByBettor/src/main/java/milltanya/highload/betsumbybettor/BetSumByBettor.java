package milltanya.highload.betsumbybettor;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class BetSumByBettor {

	public static void main(String[] args) {
		new SpringApplicationBuilder(BetSumByBettor.class).headless(false).run(args);
	}

}

