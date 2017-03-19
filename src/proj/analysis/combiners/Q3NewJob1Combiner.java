package proj.analysis.combiners;

import proj.analysis.util.Constants;
import proj.analysis.util.Q3CustomDataType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q3NewJob1Combiner extends Reducer<Text, Q3CustomDataType, Text, Q3CustomDataType> {

    @Override
    protected void reduce(Text key, Iterable<Q3CustomDataType> values, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        Map<String, Integer> yearwiseSum = new HashMap<>();
        for (Q3CustomDataType value : values) {
            IntWritable count = value.getCount();
            String property = value.getProperty().toString();
            if (count.get() == 0) {
                context.write(key, value);
            } else {
                Integer sum = yearwiseSum.get(property);
                if (sum == null) {
                    yearwiseSum.put(property, 1);
                } else {
                    yearwiseSum.put(property, sum + 1);
                }
            }
        }
        for (Map.Entry<String, Integer> entrySet : yearwiseSum.entrySet()) {
            String year = entrySet.getKey();
            Integer sum = entrySet.getValue();
            context.write(key, new Q3CustomDataType(new Text(year), new IntWritable(sum)));
        }
    }
}
