package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinterDemo extends Configured implements Tool {
    @Override
    public int run(String[] strings) {
        Configuration configuration = getConf();
        configuration.forEach(e -> System.out.println(String.format(" %s -> %s ", e.getKey(), e.getValue())));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(ConfigurationPrinterDemo.class.newInstance(), args);
        System.exit(exitCode);
    }
}
