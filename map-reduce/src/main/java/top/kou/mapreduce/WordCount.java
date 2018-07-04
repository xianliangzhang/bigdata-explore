package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.kou.mapreduce.wc.WordCountMapper;
import top.kou.mapreduce.wc.WordCountReducer;

import java.util.Arrays;

public class WordCount {
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);
    private static final String file_input_path = "/Users/hack/lab/bigdata-explore/map-reduce/src/main/resources/word.txt";
    private static final String file_output_path = "word-count-output";

    public static void main(String[] args) throws Exception {
        logger.info("WordCount: {}", Arrays.deepToString(args));

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args.length == 2 ? args[0] : file_input_path));
        FileOutputFormat.setOutputPath(job, new Path(args.length == 2 ? args[1] : file_output_path));

        System.out.println(job.waitForCompletion(true) ? "- 任务执行完成" : "- 任务执行失败");
    }
}
