package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxTemperatureDemo {
    private static final String ncdc = "/Users/hack/Workspace/bigdata-explore/map-reduce/src/main/resources/temperature.txt";

    static class Temperature implements Writable {
        private Text station;
        private IntWritable year;
        private IntWritable temperature;

        public static Temperature parse(String station, int year, int temp) {
            Temperature temperature = new Temperature();
            temperature.station = new Text(station);
            temperature.year = new IntWritable(year);
            temperature.temperature = new IntWritable(temp);
            return temperature;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Temperature that = (Temperature) o;
            return Objects.equals(station, that.station) && Objects.equals(year, that.year) && Objects.equals(temperature, that.temperature);
        }

        @Override
        public int hashCode() {
            return Objects.hash(station, year, temperature);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.station.write(dataOutput);
            this.year.write(dataOutput);
            this.temperature.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.station.readFields(dataInput);
            this.year.readFields(dataInput);
            this.temperature.readFields(dataInput);
        }
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.getLength() != 14) {
                ;
            }
            else {
                String temp= value.toString();
                //String station = temp.substring(0, 4);
                String year = temp.substring(4, 8);
                String temperature = temp.substring(12);
                context.write(new IntWritable(Integer.parseInt(year)), new IntWritable(Integer.parseInt(temperature)));
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxTemp = Integer.MIN_VALUE;
            for (IntWritable temp : values) {
                maxTemp = Math.max(maxTemp, temp.get());
            }
            context.write(key, new IntWritable(maxTemp));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureDemo <input> <output>");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MaxTemperatureDemo.class);
        job.setJobName("Max-Temperature");

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(ncdc));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
