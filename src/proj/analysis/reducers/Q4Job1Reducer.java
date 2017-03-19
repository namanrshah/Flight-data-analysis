package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q3CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Rajiv
 */
public class Q4Job1Reducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        int actualDelayCount = 0;
        int totalFlightCount = 0;
        int sumDelayMins = 0;
        String city = null;
        for (Text value : values) {
            try {
                int delayMin = Integer.parseInt(value.toString());
                totalFlightCount++;
                sumDelayMins += delayMin;
                if (delayMin > 0) {
                    actualDelayCount++;
                }
            } catch (Exception ex) {
                city = value.toString();
            }
        }
        if (city != null) {
//            context.write(NullWritable.get(), new Text(keyString + Constants.SEPARATORS.QUESTION_4_SEPARATOR + Integer.toString(totalFlightCount) + Constants.SEPARATORS.QUESTION_4_SEPARATOR + Integer.toString(actualDelayCount) + Constants.SEPARATORS.QUESTION_4_SEPARATOR + Integer.toString(sumDelayMins)));
            context.write(NullWritable.get(), new Text(city + Constants.SEPARATORS.QUESTION_4_SEPARATOR + Integer.toString(totalFlightCount) + Constants.SEPARATORS.QUESTION_4_SEPARATOR + Integer.toString(actualDelayCount) + Constants.SEPARATORS.QUESTION_4_SEPARATOR + Integer.toString(sumDelayMins)));
        }
    }
}
