package org.infinispan.configuration.cache;

/**
*
* AbstractStoreConfigurationChildBuilder delegates {@link StoreConfigurationChildBuilder} methods to a specified {@link CacheStoreConfigurationBuilder}
*
* @author Tristan Tarrant
* @since 5.2
*/
public abstract class AbstractStoreConfigurationChildBuilder<S> extends AbstractLoaderConfigurationChildBuilder<S> implements StoreConfigurationChildBuilder<S> {

   private final CacheStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ? extends CacheStoreConfigurationBuilder<?, ?>> builder;

   protected AbstractStoreConfigurationChildBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public AsyncStoreConfigurationBuilder<S> async() {
      return (AsyncStoreConfigurationBuilder<S>) builder.async();
   }

   @Override
   public SingletonStoreConfigurationBuilder<S> singletonStore() {
      return (SingletonStoreConfigurationBuilder<S>) builder.singletonStore();
   }

   @Override
   public S fetchPersistentState(boolean b) {
      return (S) builder.fetchPersistentState(b);
   }

   @Override
   public S ignoreModifications(boolean b) {
      return (S) builder.ignoreModifications(b);
   }

   @Override
   public S purgeOnStartup(boolean b) {
      return (S) builder.purgeOnStartup(b);
   }

   @Override
   public S purgerThreads(int i) {
      return (S) builder.purgerThreads(i);
   }

   @Override
   public S purgeSynchronously(boolean b) {
      return (S) builder.purgeSynchronously(b);
   }

}
