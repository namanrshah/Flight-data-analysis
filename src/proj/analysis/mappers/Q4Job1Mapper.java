package proj.analysis.mappers;

import proj.analysis.util.Constants;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Rajiv
 */
public class Q4Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        if (!valueString.startsWith(Constants.HEADER_STARTING_MAIN_DATA) && !valueString.startsWith(Constants.HEADER_STARTING_AIRPORTS_DATA)) {
            if (valueString.charAt(0) != Constants.FILE_DATA_INITIALS.OTHER_DATA) {
                String[] dataValues = valueString.split(Constants.SEPARATORS.DATA_FIELDS_SEPARATOR);
//            String year = dataValues[0];
                String origin = dataValues[16];
                String destination = dataValues[17];
                String weatherDelayMins = dataValues[25];
                if (!weatherDelayMins.equals(Constants.NOT_APPLICALBLE)) {
                    context.write(new Text(origin), new Text(weatherDelayMins));
                    context.write(new Text(destination), new Text(weatherDelayMins));
                }
            } else {
                String[] supp_data = valueString.split(Constants.SEPARATORS.SUPP_DATA_FIELDS_SEPARATOR);
                String iata = supp_data[1];
                String city = supp_data[5];
                context.write(new Text(iata), new Text(city));
            }
        }
    }
}
