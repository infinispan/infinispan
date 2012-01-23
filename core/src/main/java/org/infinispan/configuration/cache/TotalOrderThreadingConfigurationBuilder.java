package org.infinispan.configuration.cache;

import org.infinispan.config.ConfigurationException;
import org.infinispan.config.FluentConfiguration;

/**
 * Thread pool configuration for total order protocol
 * Date: 1/17/12
 * Time: 11:44 AM
 *
 * @author pruivo
 */
public class TotalOrderThreadingConfigurationBuilder extends AbstractTransportConfigurationChildBuilder<TotalOrderThreadingConfiguration> {

    private int corePoolSize = 1;

    private int maximumPoolSize = 8;

    private long keepAliveTime = 1000; //milliseconds

    protected TotalOrderThreadingConfigurationBuilder(TransactionConfigurationBuilder builder) {
        super(builder);
    }

    @Override
    void validate() {
        if(corePoolSize <= 0 || keepAliveTime <= 0 || maximumPoolSize <= 0) {
            throw new ConfigurationException("All the configuration values (corePoolSize, keepAliveTime, " +
                    "maximumPoolSize) must be greater than zero");
        } else if(corePoolSize > maximumPoolSize) {
            throw new ConfigurationException("Core pool size value is greater than the maximum pool size");
        }
    }

    @Override
    TotalOrderThreadingConfiguration create() {
        return new TotalOrderThreadingConfiguration(corePoolSize, maximumPoolSize, keepAliveTime);
    }

    @Override
    public ConfigurationChildBuilder read(TotalOrderThreadingConfiguration template) {
        this.corePoolSize = template.getCorePoolSize();
        this.maximumPoolSize = template.getMaximumPoolSize();
        this.keepAliveTime = template.getKeepAliveTime();
        return this;
    }

    public TotalOrderThreadingConfigurationBuilder corePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
        return this;
    }

    public TotalOrderThreadingConfigurationBuilder maximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
        return this;
    }

    public TotalOrderThreadingConfigurationBuilder keepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
        return this;
    }
}
