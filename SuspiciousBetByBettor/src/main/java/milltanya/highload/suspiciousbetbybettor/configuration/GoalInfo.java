package milltanya.highload.suspiciousbetbybettor.configuration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.curs.counting.model.Score;

@Getter
@RequiredArgsConstructor
public class GoalInfo {
    Score currentScore;
    String goal;
    Long timestamp;

    public GoalInfo(Score score, String g, Long ts) {
        currentScore = score;
        goal = g;
        timestamp = ts;
    }

    public String scorerTeam() {
        return goal.equals("1:0") ? "H" : goal.equals("0:1") ? "A" : "D";
    }
}
