package top.kou.mapreduce.mt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(MaxTemperatureMapper.class);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("Before Mapper: key={}, value={}", key, value);

        String ling = value.toString();
        String year = ling.substring(0, 4);
        String temp = ling.substring(8);
        context.write(new Text(year), new IntWritable(Integer.parseInt(temp)));

        logger.info("After Mapper: key={}, value={}", year, temp);
    }
}


