package com.github.thmarx.mongo.trigger;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class Configuration {
    public int connectRetries = 10;
    public long connectRetryDelay = 1000;
}
