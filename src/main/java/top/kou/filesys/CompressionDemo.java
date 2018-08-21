package top.kou.filesys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;

public class CompressionDemo {
    private static final String path = "compression.txt";

    private static void preCompress() throws IOException {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        if (!file.createNewFile()) {
            throw new RuntimeException("Create File Error: " + file);
        }

        FileWriter writer = new FileWriter(file);
        for (int i = 0; i < 10000; i++) {
            writer.write(UUID.randomUUID().toString().concat("\n"));
        }
        writer.close();

        System.out.println(String.format("File Created: %s, %d", file, file.length()));
    }

    private static void compress(Class<?> codec) {

    }

    private static void postCompress() {

    }

    public static void main(String[] args) throws Exception {
        String uri = "hdfs://localhost:9000/user/hadoop/compression.txt.gz";
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        CompressionCodec compressionCodec = factory.getCodec(path);
        InputStream ins = compressionCodec.createInputStream(fs.open(path));
        OutputStream out = fs.create(new Path(CompressionCodecFactory.removeSuffix(uri, compressionCodec.getDefaultExtension())));

        int read = 0;
        byte[] bytes = new byte[1024];
        while ((read = ins.read(bytes)) > -1) {
            out.write(bytes, 0, read);
        }
        out.flush();
        out.close();
        ins.close();
    }

}
