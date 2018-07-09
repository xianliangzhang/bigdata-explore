package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import java.io.IOException;

public class SortBySequenceFileDemo {

    static class SortBySequenceFileMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            if (text.getLength() > 0 && text.toString().trim().length() > 0) {
                IntWritable temp = new IntWritable(Integer.parseInt(text.toString().substring(8)));
                System.out.println(" -- " + temp);
                outputCollector.collect(temp, text);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(new Configuration(), SortBySequenceFileDemo.class);

        jobConf.setMapperClass(SortBySequenceFileMapper.class);
        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setNumReduceTasks(0);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(jobConf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(jobConf, GzipCodec.class);

        FileInputFormat.addInputPath(jobConf, new Path(args.length == 0 ? CommonUtils.getLocalPath("temperature.txt") : args[0]));
        SequenceFileOutputFormat.setOutputPath(jobConf, new Path("output"));

        JobClient.runJob(jobConf).waitForCompletion();
    }

}
