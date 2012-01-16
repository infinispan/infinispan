package org.infinispan.configuration.cache;

public class ClusteringConfiguration {
   
   private final CacheMode cacheMode;
   private final AsyncConfiguration asyncConfiguration;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final SyncConfiguration syncConfiguration;
   
   ClusteringConfiguration(CacheMode cacheMode, AsyncConfiguration asyncConfiguration, HashConfiguration hashConfiguration,
         L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration, SyncConfiguration syncConfiguration) {
      this.cacheMode = cacheMode;
      this.asyncConfiguration = asyncConfiguration;
      this.hashConfiguration = hashConfiguration;
      this.l1Configuration = l1Configuration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.syncConfiguration = syncConfiguration;
   }

   public CacheMode cacheMode() {
      return cacheMode;
   }
   
   public String cacheModeString() {
      return cacheMode == null ? "none" : cacheMode.toString();
   }
   
   public AsyncConfiguration async() {
      return asyncConfiguration;
   }
   
   public HashConfiguration hash() {
      return hashConfiguration;
   }
   
   public L1Configuration l1() {
      return l1Configuration;
   }
   
   public SyncConfiguration sync() {
      return syncConfiguration;
   }
   
   public StateRetrievalConfiguration stateRetrieval() {
      return new StateRetrievalConfiguration(stateTransferConfiguration);
   }

   public StateTransferConfiguration stateTransfer() {
      return stateTransferConfiguration;
   }
}
