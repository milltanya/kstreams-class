package milltanya.highload.suspiciousbetbybettor.configuration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.curs.counting.model.Score;

@Getter
@RequiredArgsConstructor
public class GoalInfo {
    Score currentScore;
    String scorerTeam;
    Long timestamp;

    public GoalInfo(Score score, String team, Long ts) {
        currentScore = score;
        scorerTeam = team;
        timestamp = ts;
    }
}
