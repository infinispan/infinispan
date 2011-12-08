package org.infinispan.configuration.cache;

public class ClusteringConfigurationBuilder extends AbstractConfigurationChildBuilder<ClusteringConfiguration> implements ClusteringConfigurationChildBuilder {

   private CacheMode cacheMode = CacheMode.LOCAL;
   private final AsyncConfigurationBuilder asyncConfigurationBuilder;
   private final HashConfigurationBuilder hashConfigurationBuilder;
   private final L1ConfigurationBuilder l1ConfigurationBuilder;
   private final StateRetrievalConfigurationBuilder stateRetrievalConfigurationBuilder;
   private final SyncConfigurationBuilder syncConfigurationBuilder;

   ClusteringConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.asyncConfigurationBuilder = new AsyncConfigurationBuilder(this);
      this.hashConfigurationBuilder = new HashConfigurationBuilder(this);
      this.l1ConfigurationBuilder = new L1ConfigurationBuilder(this);
      this.stateRetrievalConfigurationBuilder = new StateRetrievalConfigurationBuilder(this);
      this.syncConfigurationBuilder = new SyncConfigurationBuilder(this);
   }

   /**
    * Cache mode. See {@link CacheMode} for information on the various cache modes available.
    */
   public ClusteringConfigurationBuilder cacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }
   
   CacheMode cacheMode() {
      return cacheMode;
   }

   @Override
   public AsyncConfigurationBuilder async() {
      if (cacheMode.isSynchronous())
         throw new IllegalStateException("Cannot configure a async for an sync cache. Set the cache mode to async first.");
      return asyncConfigurationBuilder;
   }
   
   @Override
   public HashConfigurationBuilder hash() {
      return hashConfigurationBuilder;
   }
   
   @Override
   public L1ConfigurationBuilder l1() {
      return l1ConfigurationBuilder;
   }
   
   @Override
   public StateRetrievalConfigurationBuilder stateRetrieval() {
      return stateRetrievalConfigurationBuilder;
   }
   
   @Override
   public SyncConfigurationBuilder sync() {
      if (!cacheMode.isSynchronous())
         throw new IllegalStateException("Cannot configure a sync for an async cache. Set the cache mode to sync first.");
      return syncConfigurationBuilder;
   }
   

   @Override
   void validate() {
      asyncConfigurationBuilder.validate();
      hashConfigurationBuilder.validate();
      l1ConfigurationBuilder.validate();
      stateRetrievalConfigurationBuilder.validate();
      syncConfigurationBuilder.validate();
      
   }

   @Override
   ClusteringConfiguration create() {
      return new ClusteringConfiguration(cacheMode, asyncConfigurationBuilder.create(), hashConfigurationBuilder.create(), l1ConfigurationBuilder.create(), stateRetrievalConfigurationBuilder.create(), syncConfigurationBuilder.create());
   }
   
   @Override
   public ClusteringConfigurationBuilder read(ClusteringConfiguration template) {
      this.cacheMode = template.cacheMode();
      asyncConfigurationBuilder.read(template.async());
      hashConfigurationBuilder.read(template.hash());
      l1ConfigurationBuilder.read(template.l1());
      stateRetrievalConfigurationBuilder.read(template.stateRetrieval());
      syncConfigurationBuilder.read(template.sync());
      
      return this;
   }

}
