package hadoop158.mapreduce.sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text k3, Iterable<DoubleWritable> v3, Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable v1 : v3) {
            sum += v1.get();
        }
        context.write(k3, new DoubleWritable(sum));
    }
}
