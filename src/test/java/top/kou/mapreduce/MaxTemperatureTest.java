package top.kou.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MaxTemperatureTest {


    @Test
    public void testMaxTemperatureMapper() {
        try {
            String value = "s0012015010216";
            List<Pair<IntWritable, IntWritable>> pairs = MapDriver.newMapDriver(new MaxTemperatureDemo.MaxTemperatureMapper())
                    .withInput(new LongWritable(1), new Text(value))
                    .withOutput(new IntWritable(2015), new IntWritable(16)).run();
            pairs.stream().forEach(r -> System.out.println(String.format("%s -> %s", r.getFirst(), r.getSecond())));

            Pair<IntWritable, IntWritable> pair = pairs.iterator().next();
            assert pair.getFirst().equals(new IntWritable(2015));
            assert pair.getSecond().equals(new IntWritable(16));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMaxTemperatureReducer() {
        try {
            List<Pair<IntWritable, IntWritable>> pairs = ReduceDriver.newReduceDriver(new MaxTemperatureDemo.MaxTemperatureReducer())
                    .withInput(new IntWritable(2015), Arrays.asList(new IntWritable(1), new IntWritable(2)))
                    .run();
            pairs.stream().forEach(p -> System.out.println(String.format("%s -> %s", p.getFirst(), p.getSecond())));

            Pair<IntWritable, IntWritable> pair = pairs.iterator().next();
            assert pair.getFirst().equals(new IntWritable(2015));
            assert pair.getSecond().equals(new IntWritable(2));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMapReduce() {
        try {
            List<Pair<IntWritable, IntWritable>> pairs = MapReduceDriver.newMapReduceDriver(new MaxTemperatureDemo.MaxTemperatureMapper(), new MaxTemperatureDemo.MaxTemperatureReducer())
                    .withInput(new LongWritable(1), new Text("s0012015010216"))
                    .withInput(new LongWritable(2), new Text("s0012015010217"))
                    .run();
            pairs.stream().forEach(p -> System.out.println(String.format("%s -> %s", p.getFirst(), p.getSecond())));

            Pair<IntWritable, IntWritable> pair = pairs.iterator().next();
            assert pair.getFirst().equals(new IntWritable(2015));
            assert pair.getSecond().equals(new IntWritable(17));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
