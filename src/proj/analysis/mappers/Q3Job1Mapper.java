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
public class Q3Job1Mapper extends Mapper<LongWritable, Text, Text, Q3CustomDataType> {

    final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        if (!valueString.startsWith(Constants.HEADER_STARTING_MAIN_DATA) && !valueString.startsWith(Constants.HEADER_STARTING_AIRPORTS_DATA)) {
//            if (valueString.indexOf(0) != Constants.FILE_DATA_INITIALS.OTHER_DATA) {
            String[] dataValues = valueString.split(Constants.SEPARATORS.DATA_FIELDS_SEPARATOR);
            String year = dataValues[0];
            String origin = dataValues[16];
            String destination = dataValues[17];
            context.write(new Text(Constants.KEY_INITIALS.Q3_YEAR + year), new Q3CustomDataType(new Text(origin), one));
            context.write(new Text(Constants.KEY_INITIALS.Q3_YEAR + year), new Q3CustomDataType(new Text(destination), one));
            context.write(new Text(Constants.KEY_INITIALS.Q3_AIRPORT + origin), new Q3CustomDataType(new Text(""), one));
            context.write(new Text(Constants.KEY_INITIALS.Q3_AIRPORT + destination), new Q3CustomDataType(new Text(""), one));
//            } else {
//
//            }
        }
    }
}
