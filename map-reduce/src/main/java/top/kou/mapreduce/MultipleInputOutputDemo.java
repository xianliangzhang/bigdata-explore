package top.kou.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class MultipleInputOutputDemo {

    /**
     * 2013010722
     */
    static class ShortFormatTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
            if (text == null || text.toString().trim().length() == 0) {
                outputCollector.collect(new IntWritable(-1), new IntWritable(-1));
            }

            String year = text.toString().trim().substring(0, 4);
            String temp = text.toString().trim().substring(8);
            outputCollector.collect(new IntWritable(Integer.parseInt(year)), new IntWritable(Integer.parseInt(temp)));
        }
    }

    /**
     * 2014-01-01 14
     */
    static class LongFormatTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
            if (text == null || text.toString().trim().length() == 0) {
                outputCollector.collect(new IntWritable(-1), new IntWritable(-1));
            }

            String data = text.toString().trim().replaceAll("-", "").replaceAll(" +", "");
            String year = data.substring(0, 4);
            String temp = data.substring(8);
            outputCollector.collect(new IntWritable(Integer.parseInt(year)), new IntWritable(Integer.parseInt(temp)));
        }
    }

    static class MaxTemperatureReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, NullWritable, TemperatureStatics> {

        @Override
        public void reduce(IntWritable intWritable, Iterator<IntWritable> iterator, OutputCollector<NullWritable, TemperatureStatics> outputCollector, Reporter reporter) throws IOException {
            int totalTemp = 0;
            int simpleCount = 0;
            int maxTemp = Integer.MIN_VALUE;
            int minTemp = Integer.MAX_VALUE;

            while (iterator.hasNext()) {
                IntWritable temp = iterator.next();
                simpleCount += 1;
                totalTemp += temp.get();
                minTemp = Math.min(minTemp, temp.get());
                maxTemp = Math.max(maxTemp, temp.get());
            }

            TemperatureStatics ts = new TemperatureStatics();
            ts.set(intWritable.get(), simpleCount, maxTemp, minTemp, (float) totalTemp / (float) simpleCount);
            outputCollector.collect(NullWritable.get(), ts);
        }
    }

    static class YearlyPartitioner implements Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable intWritable, IntWritable intWritable2, int i) {
            return intWritable.get() % i;
        }

        @Override
        public void configure(JobConf jobConf) {

        }
    }

    static class YearlyMultipleOutputTextOutputFormat extends MultipleTextOutputFormat<NullWritable, TemperatureStatics> {
        @Override
        protected String generateFileNameForKeyValue(NullWritable key, TemperatureStatics value, String name) {
            return "part-year-".concat(String.valueOf(value.year.get() % 3));
        }
    }

    static class TemperatureStatics implements Writable, WritableComparable<TemperatureStatics> {
        private IntWritable year = new IntWritable();
        private IntWritable simpleCount = new IntWritable();    // 样本数量
        private IntWritable maxTemperature = new IntWritable(); // 最高温度
        private IntWritable minTemperature = new IntWritable(); // 最低温度
        private FloatWritable avgTemperature = new FloatWritable(); // 平均温度

        public TemperatureStatics set(int year, int simpleCount, int maxTemp, int minTemp, float avgTemp) {
            this.year.set(year);
            this.maxTemperature.set(maxTemp);
            this.minTemperature.set(minTemp);
            this.avgTemperature.set(avgTemp);
            this.simpleCount.set(simpleCount);
            return this;
        }

        @Override
        public int compareTo(TemperatureStatics o) {
            return this.year.compareTo(o.year);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            year.write(dataOutput);
            simpleCount.write(dataOutput);
            maxTemperature.write(dataOutput);
            minTemperature.write(dataOutput);
            avgTemperature.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            year.readFields(dataInput);
            simpleCount.readFields(dataInput);
            maxTemperature.readFields(dataInput);
            minTemperature.readFields(dataInput);
            avgTemperature.readFields(dataInput);
        }

        @Override
        public String toString() {
            return String.format("Year=%s, SimpleCount=%d, MinTemperature=%d, MaxTemperature=%d, AvgTemperature=%f",
                    year.get(), simpleCount.get(), minTemperature.get(), maxTemperature.get(), avgTemperature.get());
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf();

        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(IntWritable.class);
        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(TemperatureStatics.class);

        jobConf.setReducerClass(MaxTemperatureReducer.class);
        //jobConf.setPartitionerClass(YearlyPartitioner.class);
        //jobConf.setNumReduceTasks(3);

        MultipleInputs.addInputPath(jobConf, new Path(CommonUtils.getLocalPath("temp.txt")), TextInputFormat.class, ShortFormatTemperatureMapper.class);
        MultipleInputs.addInputPath(jobConf, new Path(CommonUtils.getLocalPath("temp-x.txt")), TextInputFormat.class, LongFormatTemperatureMapper.class);


        FileOutputFormat.setOutputPath(jobConf, new Path("output"));
        jobConf.setOutputFormat(YearlyMultipleOutputTextOutputFormat.class);

        JobClient.runJob(jobConf).waitForCompletion();
    }
}
