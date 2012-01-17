package org.infinispan.configuration.cache;

import org.infinispan.config.FluentConfiguration;

public interface TransactionConfigurationChildBuilder extends ConfigurationChildBuilder {

    RecoveryConfigurationBuilder recovery();

    //Pedro -- total order
    TotalOrderThreadingConfigurationBuilder totalOrderThreading();

}
