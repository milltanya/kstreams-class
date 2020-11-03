package milltanya.highload.betsumbyteam;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class BetSumByTeam {

	public static void main(String[] args) {
		new SpringApplicationBuilder(BetSumByTeam.class).headless(false).run(args);
	}

}

