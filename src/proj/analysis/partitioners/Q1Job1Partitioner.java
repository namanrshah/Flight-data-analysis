package proj.analysis.partitioners;

import proj.analysis.util.Constants;
import proj.analysis.util.Q1CustomDataType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

public class Q1Job1Partitioner extends Partitioner<Text, Q1CustomDataType> {

    final int hours = 24;//(0-23)
    final int daysInWeek = 7;//(24-30)
    final int months = 12;//(31-42)
    //Keys are assigned in order given above

    @Override
    public int getPartition(Text key, Q1CustomDataType value, int i) {
        String keyString = key.toString();
        int paramValue = Integer.parseInt(keyString.substring(1));
        if (keyString.startsWith(Constants.KEY_INITIALS.Q12_HOUR_FOR_TIME)) {
            return (paramValue % 24);
        } else if (keyString.startsWith(Constants.KEY_INITIALS.Q12_DAY_OF_WEEK)) {
            return 24 + ((paramValue - 1) % 7);
        } else {
            return 31 + ((paramValue - 1) % 12);
        }
    }
}
