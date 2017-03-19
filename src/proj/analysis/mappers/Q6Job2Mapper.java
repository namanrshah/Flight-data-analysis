package proj.analysis.mappers;

import proj.analysis.util.Q6Job1CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class Q6Job2Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    final IntWritable one = new IntWritable(1);
    final IntWritable zero = new IntWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String valueString = value.toString();
        context.write(NullWritable.get(), value);
    }
}
