package org.infinispan.configuration.cache;

public abstract class AbstractLoaderConfigurationChildBuilder<T> extends AbstractLoadersConfigurationChildBuilder<T> implements LoaderConfigurationChildBuilder {

   private final LoaderConfigurationBuilder builder;

   private boolean purgeOnStartup;
   private boolean purgeSynchronously;
   boolean fetchPersistentState;
   boolean ignoreModifications;
   
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

   public AbstractLoaderConfigurationChildBuilder purgeOnStartup(boolean purgeOnStartup) {
      this.purgeOnStartup = purgeOnStartup;
      return this;
   }

   public AbstractLoaderConfigurationChildBuilder purgeSynchronously(boolean purgeSynchronously) {
      this.purgeSynchronously = purgeSynchronously;
      return this;
   }

   public AbstractLoaderConfigurationChildBuilder fetchPersistentState(boolean fetchPersistentState) {
      this.fetchPersistentState = fetchPersistentState;
      return this;
   }

   public AbstractLoaderConfigurationChildBuilder ignoreModifications(boolean ignoreModifications) {
      this.ignoreModifications = ignoreModifications;
      return this;
   }

}
