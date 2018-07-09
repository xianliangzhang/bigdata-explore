package top.kou.mapreduce.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextIntPairWritable implements Writable, WritableComparable<TextIntPairWritable> {
    private Text first = new Text();
    private IntWritable second = new IntWritable();


    public static class TextComparator extends WritableComparator {
        public TextComparator() {
            super(TextIntPairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextIntPairWritable) a).first.compareTo(((TextIntPairWritable) b).first);
        }
    }

    public static class TextIntComparator extends WritableComparator {
        public TextIntComparator() {
            super(TextIntPairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (((TextIntPairWritable) a).first.compareTo(((TextIntPairWritable) b).first) == 0) {
                return ((TextIntPairWritable) a).second.compareTo(((TextIntPairWritable) b).second);
            }
            return 0;
        }
    }

    public TextIntPairWritable set(String first, int second) {
        this.first.set(first);
        this.second.set(second);
        return this;
    }

    @Override
    public int compareTo(TextIntPairWritable o) {
        if (first.compareTo(o.first) == 0) {
            return second.compareTo(o.second);
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public Text getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }
}
