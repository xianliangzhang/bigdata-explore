package top.kou.common.util;

import java.io.IOException;
import java.util.Properties;

public class ConfigUtils {
    private static final String CONFIG_FILE = "config.properties";
    private static final Properties PROPERTIES = new Properties() {{
        try {
            load(ConfigUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }};

    public static String get(String key) {
        return PROPERTIES.getProperty(key);
    }

    public static String get(String key, String defaultValue) {
        return PROPERTIES.containsKey(key) ? PROPERTIES.getProperty(key) : defaultValue;
    }
}
