package top.kou.configuration;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationDemo {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set("xxoo", "true");
        System.out.println(configuration.getBoolean("xxoo", false));
    }
}
