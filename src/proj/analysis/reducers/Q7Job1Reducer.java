package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q7Job1CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q7Job1Reducer extends Reducer<Text, Q7Job1CustomDataType, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q7Job1CustomDataType> values, Context context) throws IOException, InterruptedException {
        int sumDepDelay = 0;
        int sumLateACDelay = 0;
        int sumFlightCount = 0;
        int sumLateACDelayCount = 0;
        for (Q7Job1CustomDataType value : values) {
            sumDepDelay += value.getDepdelay().get();
            sumLateACDelay += value.getLateAircraftDelay().get();
            sumFlightCount += value.getFlightCount().get();
            sumLateACDelayCount += value.getLateAircraftDelayCount().get();
        }
        context.write(NullWritable.get(), new Text(key.toString() + Constants.SEPARATORS.QUESTION_7_SEPARATOR + sumDepDelay + Constants.SEPARATORS.QUESTION_7_SEPARATOR + sumLateACDelay + Constants.SEPARATORS.QUESTION_7_SEPARATOR + sumFlightCount + Constants.SEPARATORS.QUESTION_7_SEPARATOR + sumLateACDelayCount));
    }
}
