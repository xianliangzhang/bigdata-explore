package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Join分为MapperJoin 和 ReduceJoin，本示例展示ReduceJoin
 * 原理：两个Mapper的结果打上标记发送到同一个Reducer，因为他们有相同的Key，
 * 在Reducer阶段，根据同一个Key的不同Value的标记，可以知道他们的含义，进而将他们组成相同的对象
 */
public class JoinDemo {

    static final IntWritable SOURCE_STATION = new IntWritable(1);
    static final IntWritable SOURCE_TEMPERATURE = new IntWritable(0);

    static class StationTemperatureWritable implements Writable, WritableComparable<StationTemperatureWritable> {
        private Text stationId = new Text();
        private Text stationName = new Text();
        private IntWritable year = new IntWritable();
        private IntWritable temperature = new IntWritable();
        private IntWritable mapperSource = new IntWritable();

        public static StationTemperatureWritable parse(StationTemperatureWritable st) {
            StationTemperatureWritable newSt = new StationTemperatureWritable();
            newSt.stationId.set(st.stationId);
            newSt.stationName.set(st.stationName);
            newSt.year.set(st.year.get());
            newSt.temperature.set(st.temperature.get());
            newSt.mapperSource.set(st.mapperSource.get());
            return newSt;
        }

        public static StationTemperatureWritable parseStation(String stationId, String stationName) {
            StationTemperatureWritable st = new StationTemperatureWritable();
            st.mapperSource.set(SOURCE_STATION.get());
            st.stationId.set(stationId);
            st.stationName.set(stationName);
            return st;
        }

        public static StationTemperatureWritable parseTemperature(String stationId, int year, int temp) {
            StationTemperatureWritable st = new StationTemperatureWritable();
            st.mapperSource.set(SOURCE_TEMPERATURE.get());
            st.stationId.set(stationId);
            st.year.set(year);
            st.temperature.set(temp);
            return st;
        }

        @Override
        public int compareTo(StationTemperatureWritable o) {
            if (year.equals(o.year)) return temperature.compareTo(o.temperature);
            return year.compareTo(o.year);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            stationId.write(dataOutput);
            stationName.write(dataOutput);
            year.write(dataOutput);
            temperature.write(dataOutput);
            mapperSource.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            stationId.readFields(dataInput);
            stationName.readFields(dataInput);
            year.readFields(dataInput);
            temperature.readFields(dataInput);
            mapperSource.readFields(dataInput);
        }

        @Override
        public String toString() {
            return String.format("Year=%s Station=%s Temperature=%s", year, stationName, temperature);
        }
    }

    static class StationMapper extends MapReduceBase implements Mapper<Object, Text, Text, StationTemperatureWritable> {

        @Override
        public void map(Object o, Text text, OutputCollector<Text, StationTemperatureWritable> outputCollector, Reporter reporter) throws IOException {
            String[] strings = CommonUtils.splits(text);
            StationTemperatureWritable st = StationTemperatureWritable.parseStation(strings[0], strings[1]);
            outputCollector.collect(new Text(strings[0]), st);
        }
    }

    static class TemperatureMapper extends MapReduceBase implements Mapper<Object, Text, Text, StationTemperatureWritable> {

        @Override
        public void map(Object o, Text text, OutputCollector<Text, StationTemperatureWritable> outputCollector, Reporter reporter) throws IOException {
            String stationId = text.toString().substring(0, 4);
            String year = text.toString().substring(4, 8);
            String temperature = text.toString().substring(12);
            StationTemperatureWritable st = StationTemperatureWritable.parseTemperature(stationId, Integer.parseInt(year), Integer.parseInt(temperature));
            outputCollector.collect(new Text(stationId), st);
        }
    }

    static class StationPartitioner implements Partitioner<Text, StationTemperatureWritable> {

        @Override
        public int getPartition(Text text, StationTemperatureWritable stationTemperatureWritable, int i) {
            return text.hashCode() % i;
        }

        @Override
        public void configure(JobConf jobConf) {

        }
    }

    static class JoinReducer extends MapReduceBase implements Reducer<Text, StationTemperatureWritable, StationTemperatureWritable, NullWritable> {

        @Override
        public void reduce(Text text, Iterator<StationTemperatureWritable> iterator, OutputCollector<StationTemperatureWritable, NullWritable> outputCollector, Reporter reporter) throws IOException {
            List<StationTemperatureWritable> temps = new LinkedList<>();
            Text statName = new Text();
            while (iterator.hasNext()) {
                StationTemperatureWritable temp = StationTemperatureWritable.parse(iterator.next());
                if (temp.mapperSource.equals(SOURCE_TEMPERATURE)) {
                    temps.add(temp);
                } else {
                    statName.set(temp.stationName);
                }
            }
            for (StationTemperatureWritable st : temps) {
                st.stationName.set(statName);
                outputCollector.collect(st, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(new Configuration(), JoinDemo.class);
        jobConf.setNumReduceTasks(1);

        MultipleInputs.addInputPath(jobConf, new Path(CommonUtils.getLocalPath("temperature.txt")), TextInputFormat.class, TemperatureMapper.class);
        MultipleInputs.addInputPath(jobConf, new Path(CommonUtils.getLocalPath("station.txt")), TextInputFormat.class, StationMapper.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(StationTemperatureWritable.class);
        jobConf.setPartitionerClass(StationPartitioner.class);

        jobConf.setReducerClass(JoinReducer.class);
        jobConf.setOutputKeyClass(StationTemperatureWritable.class);
        jobConf.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(jobConf, new Path("output"));

        JobClient.runJob(jobConf).waitForCompletion();
    }
}
