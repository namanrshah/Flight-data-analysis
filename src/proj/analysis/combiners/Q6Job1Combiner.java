package proj.analysis.combiners;

import proj.analysis.util.Q6Job1CustomDataType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q6Job1Combiner extends Reducer<Text, Q6Job1CustomDataType, Text, Q6Job1CustomDataType> {

    @Override
    protected void reduce(Text key, Iterable<Q6Job1CustomDataType> values, Context context) throws IOException, InterruptedException {

        Map<String, List<Integer>> yearwiseValues = new HashMap<>();
        for (Q6Job1CustomDataType value : values) {
            int countVal = value.getCount().get();
            if (countVal != 0) {
                Text year = value.getYear();
                List<Integer> stats = yearwiseValues.get(year.toString());
                if (stats == null) {
                    stats = new ArrayList<>();
                    stats.add(0);
                    stats.add(0);
                    yearwiseValues.put(year.toString(), stats);
                }
                stats.set(0, stats.get(0) + value.getSum().get());
                stats.set(1, stats.get(1) + 1);
            } else {
                context.write(key, value);
            }
        }
        for (Map.Entry<String, List<Integer>> entrySet : yearwiseValues.entrySet()) {
            String year = entrySet.getKey();
            List<Integer> stats = entrySet.getValue();
            context.write(key, new Q6Job1CustomDataType(new Text(year), new IntWritable(stats.get(0)), new IntWritable(stats.get(1))));
        }
    }
}
