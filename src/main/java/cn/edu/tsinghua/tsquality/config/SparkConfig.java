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

  private final org.apache.hadoop.conf.Configuration hadoopConf;

  public SparkConfig(org.apache.hadoop.conf.Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Bean
  public SparkConf sparkConf() {
    SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(masterUrl);
    hadoopConf.forEach(entry -> sparkConf.set(entry.getKey(), entry.getValue()));
    return sparkConf;
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
