package org.infinispan.configuration.cache;

public interface LoaderConfigurationChildBuilder extends ConfigurationChildBuilder {

   public AsyncLoaderConfigurationBuilder async();
   
   public SingletonStoreConfigurationBuilder singletonStore();
   
}
