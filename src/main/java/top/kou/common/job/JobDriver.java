package top.kou.common.job;

import org.apache.hadoop.util.Tool;
import top.kou.common.util.ConfigUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 通用启动类，前三个参数固定，后面的参数为 hdfs 配置参数
 * 第一个参数：作业名称
 * 第二个参数：input
 * 第三个参数：output
 */
public class JobDriver {

    private static String[] getJobArgs(String[] srcArgs) {
        String input = srcArgs.length >= 1 ? srcArgs[0] : ConfigUtils.get("default.job.input");
        String output = srcArgs.length >= 2 ? srcArgs[1] : ConfigUtils.get("default.job.output");
        String[] targetArgs = new String[2 + srcArgs.length > 2 ? srcArgs.length - 2 : 0];
        targetArgs[0] = input;
        targetArgs[1] = output;
        System.arraycopy(srcArgs, 2, targetArgs, 2, srcArgs.length - 2);
        return targetArgs;
    }

    private static Map<String, Class<? extends Tool>> lookupDrivableJobClasses() {
        Map<String, Class<? extends Tool>> container = new HashMap<>();
        System.out.println( Thread.currentThread().getContextClassLoader().getResource(".") + " +++ ");
        String basePack = JobDriver.class.getResource("/").getFile();
        lookupDrivableJobClasses(container, basePack, basePack);
        return container;
    }

    private static void lookupDrivableJobClasses(Map<String, Class<? extends Tool>> container, String basePack, String pack) {
        try {
            Files.list(Paths.get(pack)).forEach(path -> {
                if (path.toString().endsWith(".class")) {
                    collectDrivableJobClasses(container, path.toString().substring(basePack.length()));
                }
                if (path.toFile().isDirectory()) {
                    lookupDrivableJobClasses(container, basePack, path.toString());
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void collectDrivableJobClasses(Map<String, Class<? extends Tool>> container, String clazz) {
        try {
            String clazzName = clazz.substring(0, clazz.lastIndexOf(".")).replaceAll("\\/", ".");
            Class<Tool> cls = (Class<Tool>) Class.forName(clazzName);
            DrivableJob drivableJob = cls.getAnnotation(DrivableJob.class);
            if (drivableJob != null && drivableJob.name().length() > 0) {
                container.putIfAbsent(drivableJob.name(), cls);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, Class<? extends Tool>> container = lookupDrivableJobClasses();
        if (args.length < 1) {
            System.err.println("Usage: <JobName> <InputPath> <OutputPath>");
        }
        if (!container.containsKey(args[0])) {
            System.err.println("JobClass Not Found: " + args[0]);
        }
        Class<? extends Tool> tool = container.get(args[0]);
        System.exit(tool.newInstance().run(getJobArgs(Arrays.copyOfRange(args, 1, args.length - 1))));
    }
}
