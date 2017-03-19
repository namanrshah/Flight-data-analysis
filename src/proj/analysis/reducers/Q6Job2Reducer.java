package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q6CustomClass;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q6Job2Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    final IntWritable one = new IntWritable(1);

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Q6CustomClass> mapForWrite = new TreeMap<>();
        int totSum = 0;
        int totCount = 0;
        for (Text value : values) {
            String valueString = value.toString();
            String[] splitData = valueString.split("\t");
            int age = Integer.parseInt(splitData[0]);
            String statsStr = splitData[1];
            String[] splitStats = statsStr.split(Constants.SEPARATORS.QUESTION_6_SEPARATOR);
            int sum = Integer.parseInt(splitStats[0]);
            int count = Integer.parseInt(splitStats[1]);
            Q6CustomClass stats = mapForWrite.get(age);
            if (stats == null) {
                stats = new Q6CustomClass(0, 0);
                mapForWrite.put(age, stats);
            }
            stats.setSum(stats.getSum() + sum);
            stats.setCount(stats.getCount() + count);
            totCount += count;
            totSum += sum;
        }
        context.write(NullWritable.get(), new Text(String.format("%-10s%-15s%-15s%-15s%-15s%-15s%-25s", "Age", "Sum", "% Sum", "Count", "% Count", "Avg. Delay", "% Sum / % Count")));
        for (Map.Entry<Integer, Q6CustomClass> entrySet : mapForWrite.entrySet()) {
            Integer age = entrySet.getKey();
            Q6CustomClass stats = entrySet.getValue();
            stats.setPercentCount(stats.getCount() * 100.0f / totCount);
            stats.setPercentSum(stats.getSum() * 100.0f / totSum);
            context.write(NullWritable.get(), new Text(String.format("%-10s%-15s%-15s%-15s%-15s%-15s%-25s", age, stats.getSum(), stats.getPercentSum() + "%", stats.getCount(), stats.getPercentCount() + "%", stats.getSum() * 1.0f / stats.getCount(), stats.getPercentSum() / stats.getPercentCount())));
        }
    }
}
