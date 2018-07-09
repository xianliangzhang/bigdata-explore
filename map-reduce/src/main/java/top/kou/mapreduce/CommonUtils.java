package top.kou.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;

public final class CommonUtils {
    public static final String DEFAULT_LOCAL_FILE_PATH = "/Users/hack/lab/bigdata-explore/map-reduce/src/main/resources/";
    public static final String DEFAULT_LOCAL_HDFS_PREFIX = "hdfs://localhost:9000";

    public static final String getLocalPath(String path) {
        if (path.startsWith(DEFAULT_LOCAL_FILE_PATH)) {
            return path;
        }
        return DEFAULT_LOCAL_FILE_PATH.concat(path);
    }

    public static final String[] splits(Text text) {
        if (text == null || text.toString().trim().replaceAll(" +", " ").length() == 0) {
            return new String[0];
        }
        return text.toString().trim().replaceAll(" +", " ").split(" ");
    }

    public static String loadSequenceFileContent(String file) {
        try {
            Configuration configuration = new Configuration();
            SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(new Path(file)));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

            StringBuilder sb = new StringBuilder();
            while (reader.next(key, value)) {
                sb.append(key).append(" ").append(value).append("\n");
            }
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: <class> [<input>] [<output>]");
            System.exit(1);
        }

        String[] newArgs = new String[args.length - 1];
        if (args.length > 1) {
            System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        }
        Object result = dispatch(args[0].trim(), newArgs);
        assert result == null;
    }

    private static Object dispatch(String className, String... args) {
        try {
            String pkgName = CommonUtils.class.getPackage().getName();
            String targetClassName = className.startsWith(pkgName) ? className : pkgName.concat(".").concat(className);
            Class<?> clazz = Class.forName(targetClassName);
            Method method = clazz.getMethod("main", String[].class);
            return method.invoke(null, (Object) args);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
