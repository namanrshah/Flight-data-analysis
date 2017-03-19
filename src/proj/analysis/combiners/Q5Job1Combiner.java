package proj.analysis.combiners;

import proj.analysis.util.Q3CustomDataType;
import proj.analysis.util.Q5CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Rajiv
 */
public class Q5Job1Combiner extends Reducer<Text, Q5CustomDataType, Text, Q5CustomDataType> {

    final Text blank = new Text("");
    final IntWritable zero = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<Q5CustomDataType> values, Context context) throws IOException, InterruptedException {
//        String keyString = key.toString();
        int sum = 0;
        int count = 0;
        for (Q5CustomDataType value : values) {
            int tempSum = value.getSum().get();
            int tempCount = value.getCount().get();
            if (tempCount != 0) {
                sum += tempSum;
                count++;
            } else {
                context.write(key, value);
            }
        }
        context.write(key, new Q5CustomDataType(new IntWritable(sum), new IntWritable(count), blank));
    }
}
