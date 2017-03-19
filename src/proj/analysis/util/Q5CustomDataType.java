package proj.analysis.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Rajiv
 */
public class Q5CustomDataType implements Writable {

    IntWritable sum;
    IntWritable count;
    Text name;

    public Q5CustomDataType() {
        sum = new IntWritable();
        count = new IntWritable();
        name = new Text();
    }

    public Q5CustomDataType(IntWritable sum, IntWritable count, Text name) {
        this.sum = sum;
        this.count = count;
        this.name = name;
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

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        sum.write(d);
        count.write(d);
        name.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        sum.readFields(di);
        count.readFields(di);
        name.readFields(di);
    }

}
