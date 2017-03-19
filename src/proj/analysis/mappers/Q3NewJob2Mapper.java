package proj.analysis.mappers;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class Q3NewJob2Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), value);
    }
}