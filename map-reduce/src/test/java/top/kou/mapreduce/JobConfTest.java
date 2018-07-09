package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class JobConfTest {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();

        String file = "file:///Users/hack/lab/bigdata-explore/output/part-00000";
        FileSystem fs = FileSystem.get(URI.create(file), new Configuration());
        SequenceFile.Reader.Option path= SequenceFile.Reader.file(new Path(file));
        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, path);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
        long pso = reader.getPosition();
        while (reader.next(key, value)) {
            String syncSeen = reader.syncSeen() ? "*" : "";
            System.out.printf("[%s%s]\t%s\t%s\n", pso, syncSeen, key, value);
            pso = reader.getPosition();
        }
        reader.close();
    }


    @Test
    public void test() {
        String file = "file:///Users/hack/lab/bigdata-explore/output/part-00000";
        System.out.println(CommonUtils.loadSequenceFileContent(file));
    }

}
