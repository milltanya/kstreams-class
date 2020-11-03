package milltanya.highload.suspiciousbetbybettor.configuration;

import lombok.Getter;
import ru.curs.counting.model.Bet;

@Getter
public class SuspiciousBet {
    Bet bet;
    Long goalTimestamp;

    public SuspiciousBet(Bet b, Long ts) {
        bet = b;
        goalTimestamp = ts;
    }
}