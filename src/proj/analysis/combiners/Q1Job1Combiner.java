package proj.analysis.combiners;

import proj.analysis.util.Q1CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q1Job1Combiner extends Reducer<Text, Q1CustomDataType, Text, Q1CustomDataType> {

    @Override
    protected void reduce(Text key, Iterable<Q1CustomDataType> values, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        int sum = 0;
        int count = 0;
        int max = 0;
        int cancelCount = 0;
        for (Q1CustomDataType value : values) {
            int delay = value.getTotalDelay().get();
            sum += delay;
            count += value.getCount().get();
            if (max < delay) {
                max = delay;
            }
            cancelCount += value.getCancelCount().get();
        }
        context.write(key, new Q1CustomDataType(new IntWritable(sum), new IntWritable(count), new IntWritable(max), new IntWritable(cancelCount)));
    }
}
