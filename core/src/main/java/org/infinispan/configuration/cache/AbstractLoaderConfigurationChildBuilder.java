package org.infinispan.configuration.cache;

public abstract class AbstractLoaderConfigurationChildBuilder<T> extends AbstractLoadersConfigurationChildBuilder<T> implements LoaderConfigurationChildBuilder {
   
   private final AbstractLoaderConfigurationBuilder<? extends AbstractLoaderConfiguration> loaderConfigurationBuilder;
   
   AbstractLoaderConfigurationChildBuilder(AbstractLoaderConfigurationBuilder<? extends AbstractLoaderConfiguration> builder) {
      super(builder.getLoadersBuilder());
      this.loaderConfigurationBuilder = builder;
   }

   public AsyncLoaderConfigurationBuilder async() {
      return loaderConfigurationBuilder.async();
   }
   
   public SingletonStoreConfigurationBuilder singletonStore() {
      return loaderConfigurationBuilder.singletonStore();
   }

}
