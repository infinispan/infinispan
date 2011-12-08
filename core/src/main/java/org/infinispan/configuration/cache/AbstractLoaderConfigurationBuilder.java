package org.infinispan.configuration.cache;

/*
 * This is slightly different AbstractLoaderConfigurationChildBuilder, as it instantiates a new set of children (async and singletonStore)
 * rather than delegate to existing ones. 
 * 
 */
public abstract class AbstractLoaderConfigurationBuilder<T extends AbstractLoaderConfiguration> extends AbstractLoadersConfigurationChildBuilder<T> {

   protected final AsyncLoaderConfigurationBuilder async;
   protected final SingletonStoreConfigurationBuilder singletonStore;

   public AbstractLoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      this.async = new AsyncLoaderConfigurationBuilder(this);
      this.singletonStore = new SingletonStoreConfigurationBuilder(this);
   }

   public AsyncLoaderConfigurationBuilder async() {
      return async;
   }

   public SingletonStoreConfigurationBuilder singletonStore() {
      return singletonStore;
   }

}