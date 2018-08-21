package top.kou.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TextPairWritable implements WritableComparable<TextPairWritable> {
    private Text first;
    private Text second;

    public static final TextPairWritable parse(Text first, Text second) {
        TextPairWritable tpw = new TextPairWritable();
        tpw.first = first;
        tpw.second = second;
        return tpw;
    }

    public static class FirstComparator extends WritableComparator {

        public FirstComparator() {
            super(TextPairWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + Text.Comparator.readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b1[s2]) + Text.Comparator.readVInt(b2, s2);
                return Text.Comparator.compareBytes(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPairWritable && b instanceof TextPairWritable) {
                return ((TextPairWritable) a).first.compareTo(((TextPairWritable) b).first);
            }
            return super.compare(a, b);
        }
    }

    @Override
    public int compareTo(TextPairWritable o) {
        int r = first.compareTo(o.first);
        return r != 0 ? r : o.second.compareTo(o.second);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPairWritable that = (TextPairWritable) o;
        return Objects.equals(first, that.first) && Objects.equals(second, that.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }
}
