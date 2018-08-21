package top.kou.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TemperatureWritable implements WritableComparable<TemperatureWritable> {
    private Text srcText;
    private Text station;
    private Text instant;
    private IntWritable temperature;

    public static final TemperatureWritable parse(String srcText) {
        TemperatureWritable temperature = new TemperatureWritable();
        temperature.srcText = new Text(srcText);
        temperature.station = new Text(srcText.substring(0, 4));
        temperature.instant = new Text(srcText.substring(4, 12));
        temperature.temperature = new IntWritable(Integer.parseInt(srcText.substring(12)));
        return temperature;
    }

    @Override
    public int compareTo(TemperatureWritable o) {
        return Objects.compare(srcText, o.srcText, new Text.Comparator());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        srcText.write(dataOutput);
        station.write(dataOutput);
        instant.write(dataOutput);
        temperature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        srcText.readFields(dataInput);
        station.readFields(dataInput);
        instant.readFields(dataInput);
        temperature.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemperatureWritable that = (TemperatureWritable) o;
        return Objects.equals(srcText, that.srcText) &&
                Objects.equals(station, that.station) &&
                Objects.equals(instant, that.instant) &&
                Objects.equals(temperature, that.temperature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcText, station, instant, temperature);
    }

    public Text getSrcText() {
        return srcText;
    }

    public Text getStation() {
        return station;
    }

    public Text getInstant() {
        return instant;
    }

    public IntWritable getTemperature() {
        return temperature;
    }
}
