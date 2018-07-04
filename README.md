# bigdata-explore

环境总是汇总
1、native 包的问题，由于官方编译包提供的只支持 *nix 不支持 MACOS，所以 MACOS 下需要取源码编译，
   将编译生成的native包替换官方包的navive。
2、URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory()); 不生效，是因为pom
   引用的 hadoop 版本与安装的library版本不一致导致。
3、hadoop 正常运行，但通过 java 命令或 IDE 启动时缺 library，此时将安装目录的 native 下库拷贝
   到 java 扩展包位置下，如果不知道位置在哪里，可以通过 System.getPropertiey("java.library.path")
   获取。
