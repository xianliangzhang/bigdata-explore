package top.kou.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(WordCountMapper.class);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        logger.debug("Before Mapper: key={}, value={}", key, value);

        if (null == value || value.getLength() == 0 || "".equals(value.toString().trim())) {
            context.write(new Text(""), new IntWritable(1));
        } else {
            String[] words = value.toString().trim().replaceAll(" +", " ").split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }

        logger.debug("After Mapper...");
    }
}
