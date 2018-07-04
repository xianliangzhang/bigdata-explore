package top.kou.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(WordCountReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Before Reducer: key={}, value={}", key, values);

        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            count += iterator.next().get();
            System.out.println(count);
        }

        context.write(key, new IntWritable(count));
        logger.info("After Reducer: key={}, value={}", key, count);
    }
}
