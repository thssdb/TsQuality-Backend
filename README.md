# TsQuality-Backend

## 安装
1. 依赖：[Maven](https://maven.apache.org/), [MariaDB](https://mariadb.com/), [IoTDB](https://iotdb.apache.org/UserGuide/latest/QuickStart/QuickStart.html)

2. 系统配置：
   - MariaDB连接地址
   - IoTDB连接地址
   - IoTDB数据目录路径

3. 编译：在项目根目录下运行：

   ```shell
   mvn package -DskipTests
   ```

4. 运行: 

   ```
   java -jar ./target/TsQuality-Backend-0.0.1-SNAPSHOT.jar
   ```
