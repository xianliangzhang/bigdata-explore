package top.kou.temperature;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MaxTemperature {
    private static final Logger logger = LoggerFactory.getLogger(MaxTemperature.class);
    private static final String file_input_path = "/Users/hack/lab/bigdata-explore/max-temperature/src/main/resources/temp.txt";
    private static final String file_output_path = "max-temperature-output";

    public static void main(String[] args) throws Exception {
        logger.info("args: {}", Arrays.deepToString(args));

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "");
        job.setJarByClass(MaxTemperature.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args.length == 2 ? args[0] : file_input_path));
        FileOutputFormat.setOutputPath(job, new Path(args.length == 2 ? args[1] : file_output_path));

        System.out.println(job.waitForCompletion(true) ? "- 任务执行完成" : "- 任务执行失败");
    }
}
