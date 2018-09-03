package top.kou.filesys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;

public class GenerateTemperatureDemo {
    private static final String path = "hdfs://localhost:9000/user/hadoop/temperature.txt";

    static class Temperature {
        private LocalDate date;
        private String station;
        private int temperature;

        Temperature() {
            this.date = LocalDate.of(2000, 01, 01).plusDays(Math.round(Math.random() * 365));
            this.station = new String[]{"s001", "s002", "s003", "s004"}[(int) Math.round(Math.random() * 3)];
            this.temperature = (int) Math.round(Math.random() * 100);
        }

        @Override
        public String toString() {
            return String.format("%s%s%s\n", station, date.toString().replaceAll("-", ""), String.valueOf(temperature));
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), configuration);
        Path tempPath = new Path("/user/hadoop/temperature.txt");
        if (!fs.exists(tempPath)) {
            fs.createNewFile(tempPath);
        }

        FSDataOutputStream out = fs.append(tempPath);
        for (int i = 0; i < 100; i++) {
            Temperature temperature = new Temperature();
            System.out.print(temperature.toString());

            out.writeChars(temperature.toString());
        }
        out.hsync();
        out.close();
    }
}
