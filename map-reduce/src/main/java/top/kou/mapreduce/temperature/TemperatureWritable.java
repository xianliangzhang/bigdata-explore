package top.kou.mapreduce.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import sun.jvm.hotspot.utilities.Assert;
import top.kou.mapreduce.CommonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureWritable implements Writable, WritableComparable<TemperatureWritable> {
    private Text stationID = new Text();
    private IntWritable year = new IntWritable();
    private IntWritable temperature = new IntWritable();

    // 记录目标格式：s002014010132
    public static final TemperatureWritable parse(Text textRecord) {
        String[] arr = CommonUtils.splits(textRecord);
        Assert.that(arr.length == 3, "Bad Record Format: " + textRecord);

        TemperatureWritable temp = new TemperatureWritable();
        temp.stationID.set(arr[0]);
        temp.year.set(Integer.parseInt(arr[1].substring(0, 4)));
        temp.temperature.set(Integer.parseInt(arr[2]));
        return temp;
    }

    @Override
    public int compareTo(TemperatureWritable o) {
        return year.compareTo(o.year);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        stationID.write(dataOutput);
        year.write(dataOutput);
        temperature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        stationID.readFields(dataInput);
        year.readFields(dataInput);
        temperature.readFields(dataInput);
    }

    public Text getStationID() {
        return stationID;
    }

    public IntWritable getYear() {
        return year;
    }

    public IntWritable getTemperature() {
        return temperature;
    }
}
