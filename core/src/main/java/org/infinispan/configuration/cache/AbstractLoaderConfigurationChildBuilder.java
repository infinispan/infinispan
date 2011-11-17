package org.infinispan.configuration.cache;

public abstract class AbstractLoaderConfigurationChildBuilder<T> extends AbstractLoadersConfigurationChildBuilder<T> implements LoaderConfigurationChildBuilder {

   private final LoaderConfigurationBuilder builder;
   
   protected AbstractLoaderConfigurationChildBuilder(LoaderConfigurationBuilder builder) {
      super(builder.getLoadersBuilder());
      this.builder = builder; 
   }

   public AsyncLoaderConfigurationBuilder async() {
      return builder.async();
   }
   
   public SingletonStoreConfigurationBuilder singletonStore() {
      return builder.singletonStore();
   }
   
}
