package proj.analysis.combiners;

import proj.analysis.util.Q6Job1CustomDataType;
import proj.analysis.util.Q7Job1CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q7Job1Combiner extends Reducer<Text, Q7Job1CustomDataType, Text, Q7Job1CustomDataType> {

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
        context.write(key, new Q7Job1CustomDataType(new IntWritable(sumDepDelay), new IntWritable(sumLateACDelay), new IntWritable(sumFlightCount), new IntWritable(sumLateACDelayCount)));
    }
}
