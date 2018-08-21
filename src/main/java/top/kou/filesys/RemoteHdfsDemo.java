package top.kou.filesys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class RemoteHdfsDemo {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");

        String uri = "/temperature.txt";
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        FSDataInputStream ins = fs.open(new Path(uri));
        BufferedReader reader = new BufferedReader(new InputStreamReader(ins));

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        reader.close();
    }

}
