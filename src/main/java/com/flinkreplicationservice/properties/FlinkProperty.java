package com.flinkreplicationservice.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "flink.job")
public class FlinkProperty {
    private int parallelism;           // количество потоков одновременных
    private int maxRetries = 3;        // количество повторных попыток подключения
    private long retryDelayMs = 1000;  // задержка между попытками в миллисекундах
    private String cron;               // время запусков флинк джобы
}
