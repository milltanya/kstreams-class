package milltanya.highload.suspiciousbetbybettor;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class SuspiciousBetByBettor {

	public static void main(String[] args) {
		new SpringApplicationBuilder(SuspiciousBetByBettor.class).headless(false).run(args);
	}

}

