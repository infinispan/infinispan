package org.infinispan.configuration.cache;

public class ClusteringConfiguration {
   
   private final CacheMode cacheMode;
   private final AsyncConfiguration asyncConfiguration;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateRetrievalConfiguration stateRetrievalConfiguration;
   private final SyncConfiguration syncConfiguration;
   
   ClusteringConfiguration(CacheMode cacheMode, AsyncConfiguration asyncConfiguration, HashConfiguration hashConfiguration,
         L1Configuration l1Configuration, StateRetrievalConfiguration stateRetrievalConfiguration, SyncConfiguration syncConfiguration) {
      this.cacheMode = cacheMode;
      this.asyncConfiguration = asyncConfiguration;
      this.hashConfiguration = hashConfiguration;
      this.l1Configuration = l1Configuration;
      this.stateRetrievalConfiguration = stateRetrievalConfiguration;
      this.syncConfiguration = syncConfiguration;
   }

   public CacheMode getCacheMode() {
      return cacheMode;
   }
   
   public String getCacheModeString() {
      return cacheMode == null ? "none" : cacheMode.toString();
   }
   
   public AsyncConfiguration getAsync() {
      return asyncConfiguration;
   }
   
   public HashConfiguration getHash() {
      return hashConfiguration;
   }
   
   public L1Configuration getL1() {
      return l1Configuration;
   }
   
   public SyncConfiguration getSync() {
      return syncConfiguration;
   }
   
   public StateRetrievalConfiguration getStateRetrieval() {
      return stateRetrievalConfiguration;
   }

}
