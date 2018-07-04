package top.kou.fst;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class GodFileSystem {
    private static final String uri_prefix = "hdfs://koyou.top:9000";
    private static final String uri_path = "/readme.txt";

    public static void main(String[] args) throws IOException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL("hdfs://koyou.top:9000/readme.txt");
        try (InputStream in = url.openStream()) {
            IOUtils.copyBytes(in, System.out, 4096, false);
        }

    }
}
