package top.kou.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 以 Avro 格式实现最大气温的 MapReduce
 */
public class MaxTemperatureAvro {
    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"StringPair\",\"doc\":\"A pair of strings.\",\"fields\":[{\"name\":\"year\",\"type\":\"int\"},{\"name\":\"station\",\"type\":\"string\"}]}");

    static class MaxTemperatureAvroMapper extends Mapper<LongWritable, Text, IntWritable, GenericRecord> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    static class MaxTemperatureAvroReducer extends Reducer<IntWritable, GenericRecord, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<GenericRecord> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) {
    }

}
