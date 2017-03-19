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
public class Q3Job1Combiner extends Reducer<Text, Q3CustomDataType, Text, Q3CustomDataType> {

    @Override
    protected void reduce(Text key, Iterable<Q3CustomDataType> values, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        if (keyString.startsWith(Constants.KEY_INITIALS.Q3_YEAR)) {
            Map<String, Integer> flightFreq = new HashMap<>();
            for (Q3CustomDataType value : values) {
                String airport = value.getProperty().toString();
                int countToAdd = value.getCount().get();
                Integer count = flightFreq.get(airport);
                if (count == null) {
                    count = 0;
                }
                flightFreq.put(airport, count + countToAdd);
            }
            for (Map.Entry<String, Integer> entrySet : flightFreq.entrySet()) {
                String airport = entrySet.getKey();
                Integer count = entrySet.getValue();
                context.write(key, new Q3CustomDataType(new Text(airport), new IntWritable(count)));
            }
        } else if (keyString.startsWith(Constants.KEY_INITIALS.Q3_AIRPORT)) {
            int count = 0;
            for (Q3CustomDataType value : values) {
                ++count;
            }
            context.write(key, new Q3CustomDataType(new Text(keyString.substring(Constants.KEY_INITIALS.Q3_AIRPORT.length())), new IntWritable(count)));
        }
    }
}
