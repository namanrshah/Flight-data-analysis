package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q6Job1CustomDataType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q6Job1Reducer extends Reducer<Text, Q6Job1CustomDataType, IntWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q6Job1CustomDataType> values, Context context) throws IOException, InterruptedException {
        boolean found = false;
        Integer manuYear = null;
        List<Q6Job1CustomDataType> data = new ArrayList<>();
        for (Q6Job1CustomDataType value : values) {
            if (value.getCount().get() == 0) {
                found = true;
                manuYear = Integer.parseInt(value.getYear().toString());
            } else {
                Q6Job1CustomDataType obj = new Q6Job1CustomDataType(new Text(value.getYear().toString()), new IntWritable(value.getSum().get()), new IntWritable(value.getCount().get()));
                data.add(obj);
            }
        }
        if (found) {
            for (Q6Job1CustomDataType data1 : data) {
                int year = Integer.parseInt(data1.getYear().toString());
                int count = data1.getCount().get();
                int sum = data1.getSum().get();
                if (year >= manuYear && year <= manuYear + 100) {
                    context.write(new IntWritable(year - manuYear), new Text(Integer.toString(sum) + Constants.SEPARATORS.QUESTION_6_SEPARATOR + Integer.toString(count)));
                }
            }
        }
    }
}
