package proj.analysis.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author namanrs
 */
public class Q6Job1CustomDataType implements Writable {

    Text year;
    IntWritable sum;
    IntWritable count;

    public Q6Job1CustomDataType() {
        year = new Text();
        sum = new IntWritable();
        count = new IntWritable();
    }

    public Q6Job1CustomDataType(Text year, IntWritable sum, IntWritable count) {
        this.year = year;
        this.sum = sum;
        this.count = count;
    }

    public Text getYear() {
        return year;
    }

    public void setYear(Text year) {
        this.year = year;
    }

    public IntWritable getSum() {
        return sum;
    }

    public void setSum(IntWritable sum) {
        this.sum = sum;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        year.write(d);
        sum.write(d);
        count.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        year.readFields(di);
        sum.readFields(di);
        count.readFields(di);
    }

}
