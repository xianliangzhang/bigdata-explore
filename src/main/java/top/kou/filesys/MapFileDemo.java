package top.kou.filesys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MapFileDemo {
    private static final String path = "/tmp/mapfile.dat";

    public static void main(String[] args) throws IOException {
        String uri = args.length == 0 ? path : args[0];

        Configuration configuration = new Configuration();
        SequenceFile.Writer.Option[] writerOptions = {
                MapFile.Writer.keyClass(IntWritable.class),
                MapFile.Writer.valueClass(Text.class)
        };
        MapFile.Writer writer = new MapFile.Writer(configuration, new Path(uri), writerOptions);
        for (int i = 0; i < 100; i++) {
            IntWritable key = new IntWritable(i);
            Text value = new Text(SequenceFileDemo.data[i % SequenceFileDemo.data.length]);
            writer.append(key, value);
            System.out.println(String.format(" <<< %s - %s", key, value));
        }
        writer.close();

        MapFile.Reader reader = new MapFile.Reader(new Path(uri), configuration);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)) {
            System.out.println(String.format(" >> %s - %s", key, value));
        }
        reader.close();
    }
}
