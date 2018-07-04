package top.kou.mapreduce.mt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(MaxTemperatureReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Before Reducer: key={}, values={}", key, values);

        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            int tempMaxValue = value.get();
            if (tempMaxValue > maxValue) {
                maxValue = tempMaxValue;
            }
        }
        context.write(key, new IntWritable(maxValue));

        logger.info("After Reducer: key={}, values={}", key, String.valueOf(maxValue));
    }
}
