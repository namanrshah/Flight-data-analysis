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
public class Q3CustomDataType implements Writable {

    Text property;
    IntWritable count;

    public Q3CustomDataType() {
        property = new Text();
        count = new IntWritable();
    }

    public Q3CustomDataType(Text property, IntWritable count) {
        this.property = property;
        this.count = count;
    }

    
    public Text getProperty() {
        return property;
    }

    public void setProperty(Text property) {
        this.property = property;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        property.write(d);
        count.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        property.readFields(di);
        count.readFields(di);
    }

}
