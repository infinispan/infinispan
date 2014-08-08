package org.infinispan.configuration.cache;

/**
 * Defines clustered characteristics of the cache.
 * 
 * @author pmuir
 * 
 */
public class ClusteringConfiguration {

   private final CacheMode cacheMode;
   private final AsyncConfiguration asyncConfiguration;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final SyncConfiguration syncConfiguration;
   private final PartitionHandlingConfiguration partitionHandlingConfiguration;

   ClusteringConfiguration(CacheMode cacheMode, AsyncConfiguration asyncConfiguration, HashConfiguration hashConfiguration,
         L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration, SyncConfiguration syncConfiguration,
         PartitionHandlingConfiguration partitionHandlingStrategy) {
      this.cacheMode = cacheMode;
      this.asyncConfiguration = asyncConfiguration;
      this.hashConfiguration = hashConfiguration;
      this.l1Configuration = l1Configuration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.syncConfiguration = syncConfiguration;
      this.partitionHandlingConfiguration  = partitionHandlingStrategy;
   }

   /**
    * Cache mode. See {@link CacheMode} for information on the various cache modes available.
    */
   public CacheMode cacheMode() {
      return cacheMode;
   }

   /**
    * Configures cluster's behaviour in the presence of partitions or node failures.
    */
   public PartitionHandlingConfiguration partitionHandling() {
      return partitionHandlingConfiguration;
   }

   public String cacheModeString() {
      return cacheMode == null ? "none" : cacheMode.toString();
   }

   /**
    * Configure async sub element. Once this method is invoked users cannot subsequently invoke
    * <code>sync()</code> as two are mutually exclusive
    */
   public AsyncConfiguration async() {
      return asyncConfiguration;
   }

   /**
    * Configure hash sub element
    */
   public HashConfiguration hash() {
      return hashConfiguration;
   }

   /**
    * This method allows configuration of the L1 cache for distributed caches. When this method is
    * called, it automatically enables L1. So, if you want it to be disabled, make sure you call
    * {@link org.infinispan.configuration.cache.L1ConfigurationBuilder#disable()}
    */
   public L1Configuration l1() {
      return l1Configuration;
   }

   /**
    * Configure sync sub element. Once this method is invoked users cannot subsequently invoke
    * <code>async()</code> as two are mutually exclusive
    */
   public SyncConfiguration sync() {
      return syncConfiguration;
   }

   public StateTransferConfiguration stateTransfer() {
      return stateTransferConfiguration;
   }

   @Override
   public String toString() {
      return "ClusteringConfiguration{" +
            "async=" + asyncConfiguration +
            ", cacheMode=" + cacheMode +
            ", hash=" + hashConfiguration +
            ", l1=" + l1Configuration +
            ", stateTransfer=" + stateTransferConfiguration +
            ", sync=" + syncConfiguration +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusteringConfiguration that = (ClusteringConfiguration) o;

      if (asyncConfiguration != null ? !asyncConfiguration.equals(that.asyncConfiguration) : that.asyncConfiguration != null)
         return false;
      if (cacheMode != that.cacheMode) return false;
      if (hashConfiguration != null ? !hashConfiguration.equals(that.hashConfiguration) : that.hashConfiguration != null)
         return false;
      if (l1Configuration != null ? !l1Configuration.equals(that.l1Configuration) : that.l1Configuration != null)
         return false;
      if (stateTransferConfiguration != null ? !stateTransferConfiguration.equals(that.stateTransferConfiguration) : that.stateTransferConfiguration != null)
         return false;
      if (syncConfiguration != null ? !syncConfiguration.equals(that.syncConfiguration) : that.syncConfiguration != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = cacheMode != null ? cacheMode.hashCode() : 0;
      result = 31 * result + (asyncConfiguration != null ? asyncConfiguration.hashCode() : 0);
      result = 31 * result + (hashConfiguration != null ? hashConfiguration.hashCode() : 0);
      result = 31 * result + (l1Configuration != null ? l1Configuration.hashCode() : 0);
      result = 31 * result + (stateTransferConfiguration != null ? stateTransferConfiguration.hashCode() : 0);
      result = 31 * result + (syncConfiguration != null ? syncConfiguration.hashCode() : 0);
      return result;
   }

}
