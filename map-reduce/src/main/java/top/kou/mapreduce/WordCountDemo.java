package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordCountDemo {

    static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String[] arr = CommonUtils.splits(text);
            for (String ar : arr) {
                outputCollector.collect(new Text(ar), new IntWritable(1));
            }
        }
    }

    static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int counter = 0;
            while (iterator.hasNext()) {
                counter += iterator.next().get();
            }
            outputCollector.collect(text, new IntWritable(counter));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(new Configuration(), WordCountDemo.class);

        jobConf.setMapperClass(WordCountMapper.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(IntWritable.class);

        jobConf.setReducerClass(WordCountReducer.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(jobConf, new Path(args.length == 0 ? CommonUtils.getLocalPath("word.txt") : args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path("output"));
        JobClient.runJob(jobConf).waitForCompletion();
    }
}
