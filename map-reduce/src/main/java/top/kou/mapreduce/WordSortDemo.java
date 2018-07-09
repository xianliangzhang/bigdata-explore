package top.kou.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapred.RunningJob;

import java.io.IOException;
import java.util.Iterator;

/**
 * 将文本中的单词排序输出
 */
public class WordSortDemo {

    static class WordSortMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, NullWritable> outputCollector, Reporter reporter) throws IOException {
            String[] arr = CommonUtils.splits(text);
            for (String ar : arr) {
                outputCollector.collect(new Text(ar), NullWritable.get());
            }
        }
    }

    static class WordSortReducer extends MapReduceBase implements Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        public void reduce(Text text, Iterator<NullWritable> iterator, OutputCollector<Text, NullWritable> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(text, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(WordSortDemo.class);
        jobConf.setMapperClass(WordSortMapper.class);
        jobConf.setReducerClass(WordSortReducer.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(NullWritable.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(jobConf, new Path(args.length == 0 ? CommonUtils.getLocalPath("word.txt") : args[0] ));
        FileOutputFormat.setOutputPath(jobConf, new Path("output"));

        RunningJob rj = JobClient.runJob(jobConf);
        rj.waitForCompletion();
    }
}
