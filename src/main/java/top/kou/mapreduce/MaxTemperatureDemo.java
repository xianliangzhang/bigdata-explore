package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
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
    private static final String ncdc = "/Users/hack/Workspace/bigdata-explore/src/main/resources/temperature.txt";

    static class TemperatureWritable implements WritableComparable<TemperatureWritable> {
        private Text station;
        private IntWritable year;
        private IntWritable temperature;

        public static TemperatureWritable parse(String station, int year, int temp) {
            TemperatureWritable temperature = new TemperatureWritable();
            temperature.station = new Text(station);
            temperature.year = new IntWritable(year);
            temperature.temperature = new IntWritable(temp);
            return temperature;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TemperatureWritable that = (TemperatureWritable) o;
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

        @Override
        public int compareTo(TemperatureWritable o) {
            if (year.compareTo(o.year) > 0) return 1;
            return temperature.compareTo(o.temperature);
        }
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.getLength() != 14) {
                ;
            } else {
                String temp = value.toString();
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
        Configuration configuration = new Configuration();
        configuration.setBoolean("mapred.compress.map.output", true);
        configuration.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MaxTemperatureDemo.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        FileInputFormat.addInputPath(job, args.length >= 1 ? new Path(args[0]) : new Path(ncdc));
        FileOutputFormat.setOutputPath(job, args.length >= 2 ? new Path(args[1]) : new Path("output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
