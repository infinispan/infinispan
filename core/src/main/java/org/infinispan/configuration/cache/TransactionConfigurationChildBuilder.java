package org.infinispan.configuration.cache;

public interface TransactionConfigurationChildBuilder extends ConfigurationChildBuilder {

   RecoveryConfigurationBuilder recovery();
   
}
