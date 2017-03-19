package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q3CustomDataType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q3NewJob1Reducer extends Reducer<Text, Q3CustomDataType, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q3CustomDataType> values, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        int countSum = 0;
        String valString = "";
        String airportName = null;
        Map<String, Integer> yearwiseCount = new HashMap<>();
        for (Q3CustomDataType value : values) {
            int count = value.getCount().get();
            String prop = value.getProperty().toString();
            if (count == 0) {
                if (airportName == null) {
                    airportName = prop;
                }
            } else {
                countSum += count;
                Integer medisum = yearwiseCount.get(prop);
                if (medisum == null) {
                    yearwiseCount.put(prop, count);
                } else {
                    yearwiseCount.put(prop, count + medisum);
                }
            }
        }
        for (Map.Entry<String, Integer> entrySet : yearwiseCount.entrySet()) {
            String year = entrySet.getKey();
            Integer count = entrySet.getValue();
            valString += year + Constants.SEPARATORS.QUESTION_3_FINAL_REDUCER_SEPARATOR + count + Constants.SEPARATORS.QUESTION_3_SEPARATOR;
        }
        if (valString != null && !valString.isEmpty()) {
            valString = valString.substring(0, valString.length() - 1);
            context.write(NullWritable.get(), new Text(keyString + "-" + airportName + Constants.SEPARATORS.QUESTION_3_SEPARATOR + countSum + Constants.SEPARATORS.QUESTION_3_SEPARATOR + valString));
        }
    }
}
