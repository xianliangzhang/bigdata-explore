package top.kou.filesys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SequenceFileDemo {
    public static final String[] data = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {
        String uri = args.length == 0 ? "file:///tmp/sequence.dat" : args[0];
        Configuration configuration = new Configuration();
        Path path = new Path(uri);

        SequenceFile.Writer.Option[] writerOptions = {
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Text.class),
                SequenceFile.Writer.file(path)
        };
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration, writerOptions);
        for (int i = 0; i < 100; i++) {
            writer.append(new IntWritable(i), new Text(data[i % data.length]));
            System.out.println(String.format(" << %d - %s", i, data[i % data.length]));
        }
        writer.close();

        SequenceFile.Reader.Option[] readerOptions = {SequenceFile.Reader.file(path)};
        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, readerOptions);

        Text value = new Text();
        IntWritable key = new IntWritable();
        while (reader.next(key, value)) {
            System.out.println(String.format(">> %s, %s", key, value));
        }
        reader.close();
    }
}
