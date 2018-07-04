package top.kou.fst;

import org.apache.hadoop.io.compress.BZip2Codec;

public class CompressionTest {
    public static void main(String[] args) throws Exception {
        int x = new BZip2Codec().createCompressor().compress("Hello".getBytes(), 512, 512);
    }
}
