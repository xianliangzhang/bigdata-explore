package top.kou.filesys;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {
        String path = "/tmp/sequence.dat";
        FileReader reader = new FileReader(path);
        System.out.println((char) reader.read());
        System.out.println((char) reader.read());
        System.out.println((char) reader.read());
        System.out.println((char) reader.read());
        System.out.println((char) reader.read());
        reader.close();
    }
}
