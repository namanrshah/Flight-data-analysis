package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q5CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Rajiv
 */
public class Q5Job1Reducer extends Reducer<Text, Q5CustomDataType, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q5CustomDataType> values, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        int sum = 0;
        int count = 0;
        String name = null;
        for (Q5CustomDataType value : values) {
            int tempCount = value.getCount().get();
            if (tempCount != 0) {
                sum += value.getSum().get();
                count += tempCount;
            } else {
                if (name == null) {
                    name = value.getName().toString();
                }
            }
//            context.write(NullWritable.get(), value.getName());
        }
        if (name != null) {
            context.write(NullWritable.get(), new Text(keyString + "-" + name + Constants.SEPARATORS.QUESTION_5_SEPARATOR + Integer.toString(sum) + Constants.SEPARATORS.QUESTION_5_SEPARATOR + Integer.toString(count)));
        }
    }
}
