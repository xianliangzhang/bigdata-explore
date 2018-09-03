package top.kou.common.util;

public class PathUtils {

    private static final String JAR_IDENTIFIER = "jar!";

    public static void main(String[] args) {

        //file:/Users/hack/Workspace/bigdata-explore/target/bigdata-explore.jar!/top/kou/common/util/
        String classFilePath = PathUtils.class.getResource("").getPath();

        ///Users/hack/Workspace/bigdata-explore/target/classes/top/kou/common/util/
        System.out.println("1-" + PathUtils.class.getResource("").getPath());
    }



}
