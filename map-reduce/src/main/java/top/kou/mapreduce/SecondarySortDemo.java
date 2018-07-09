package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import top.kou.mapreduce.common.TextIntPairWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * 二次排序：对Value排序，思路是对Map阶段输出的Key做特殊处理，使Key包含Key和需要排序的Value部分，
 * 然后Key的Comparator中，先对Key做比较再对Value部分做比较，达到二次排序的目的
 */
public class SecondarySortDemo {

    static class SortMapper extends MapReduceBase implements Mapper<Object, Text, TextIntPairWritable, IntWritable> {
        private TextIntPairWritable key = new TextIntPairWritable();

        @Override
        public void map(Object o, Text text, OutputCollector<TextIntPairWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String src = text.toString();
            String station = src.substring(0, 4);
            String year = src.substring(4, 8);
            outputCollector.collect(key.set(station, Integer.parseInt(year)), key.getSecond());
        }
    }

    static class SortReducer extends MapReduceBase implements Reducer<TextIntPairWritable, IntWritable, Text, Text> {

        @Override
        public void reduce(TextIntPairWritable textIntPairWritable, Iterator<IntWritable> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (iterator.hasNext()) {
                sb.append(iterator.next().toString()).append(iterator.hasNext() ? ", " : "");
            }
            outputCollector.collect(new Text(textIntPairWritable.getFirst()), new Text(sb.toString()));
        }
    }

    static class SortPartitioner implements Partitioner<TextIntPairWritable, IntWritable> {

        @Override
        public int getPartition(TextIntPairWritable textIntPairWritable, IntWritable intWritable, int i) {
            return textIntPairWritable.getFirst().hashCode() % i;
        }

        @Override
        public void configure(JobConf jobConf) {

        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(new Configuration(), SecondarySortDemo.class);

        jobConf.setMapperClass(SortMapper.class);
        jobConf.setMapOutputKeyClass(TextIntPairWritable.class);
        jobConf.setMapOutputValueClass(IntWritable.class);

        jobConf.setReducerClass(SortReducer.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setPartitionerClass(SortPartitioner.class);
        jobConf.setOutputValueGroupingComparator(TextIntPairWritable.TextComparator.class);

        FileInputFormat.addInputPath(jobConf, new Path(args.length == 0 ? CommonUtils.getLocalPath("temperature.txt") : args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path("output"));

        JobClient.runJob(jobConf).waitForCompletion();
    }
}
