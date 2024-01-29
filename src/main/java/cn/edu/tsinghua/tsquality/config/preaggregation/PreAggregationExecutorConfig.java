package cn.edu.tsinghua.tsquality.config.preaggregation;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

@Data
@Component
@EnableScheduling
@ConfigurationProperties(prefix = "pre-aggregation.executor")
public class PreAggregationExecutorConfig {
  private int corePoolSize;
  private int maxPoolSize;
  private int queueCapacity;
  private String namePrefix;

  @Bean(name = "preAggregationTaskExecutor")
  public TaskExecutor preAggregationTaskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(corePoolSize);
    executor.setMaxPoolSize(maxPoolSize);
    executor.setQueueCapacity(queueCapacity);
    executor.setThreadNamePrefix(namePrefix);
    executor.setWaitForTasksToCompleteOnShutdown(true);
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
    executor.initialize();
    return executor;
  }
}
