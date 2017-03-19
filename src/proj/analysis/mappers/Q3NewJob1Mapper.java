package proj.analysis.mappers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q3CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class Q3NewJob1Mapper extends Mapper<LongWritable, Text, Text, Q3CustomDataType> {

    final IntWritable one = new IntWritable(1);
    final IntWritable zero = new IntWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        if (!valueString.startsWith(Constants.HEADER_STARTING_MAIN_DATA) && !valueString.startsWith(Constants.HEADER_STARTING_AIRPORTS_DATA)) {
            if (valueString.charAt(0) != Constants.FILE_DATA_INITIALS.OTHER_DATA) {
                String[] dataValues = valueString.split(Constants.SEPARATORS.DATA_FIELDS_SEPARATOR);
                String year = dataValues[0];
                String origin = dataValues[16];
                String destination = dataValues[17];
                context.write(new Text(origin), new Q3CustomDataType(new Text(year), one));
                context.write(new Text(destination), new Q3CustomDataType(new Text(year), one));
            } else {
                String[] dataValues = valueString.split(Constants.SEPARATORS.SUPP_DATA_FIELDS_SEPARATOR);
                String iata = dataValues[1];
                String airport = dataValues[3];
                context.write(new Text(iata), new Q3CustomDataType(new Text(airport), zero));
            }
        }
    }
}
