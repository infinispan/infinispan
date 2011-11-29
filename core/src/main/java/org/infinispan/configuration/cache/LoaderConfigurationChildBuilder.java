package org.infinispan.configuration.cache;

public interface LoaderConfigurationChildBuilder extends ConfigurationChildBuilder {

   AsyncLoaderConfigurationBuilder async();
   
   SingletonStoreConfigurationBuilder singletonStore();
   
}
