package top.kou.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(MaxTemperatureReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;

        logger.info("Before Reducer: key={}, values={}", key, getOriginMappedValues(values));

        for (IntWritable value : values) {
            int tempMaxValue = value.get();
            if (tempMaxValue > maxValue) {
                maxValue = tempMaxValue;
            }
        }

        logger.info("After Reducer: key={}, values={}", key, String.valueOf(maxValue));
    }

    private String getOriginMappedValues(Iterable<IntWritable> values) {
        StringBuilder builder = new StringBuilder();

        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            builder.append(iterator.next());
            if (iterator.hasNext()) {
                builder.append(",");
            }
        }
        return builder.toString();
    }
}
