package cn.edu.tsinghua.tsquality.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
  @Value("${spark.app.name:TsQuality Spark Application}")
  private String appName;

  @Value("${spark.master:local[2]}")
  private String masterUrl;

  @Value("${hdfs.defaultFS:hdfs://localhost:9000/}")
  private String defaultFS;

  @Bean
  public SparkConf sparkConf() {
    return new SparkConf()
        .setAppName(appName)
        .setMaster(masterUrl)
        .set("fs.defaultFS", defaultFS);
  }

  @Bean
  @ConditionalOnMissingBean(JavaSparkContext.class)
  public JavaSparkContext javaSparkContext() {
    return new JavaSparkContext(sparkConf());
  }

  @Bean
  public SparkSession sparkSession() {
    return SparkSession.builder().sparkContext(javaSparkContext().sc()).getOrCreate();
  }
}
