package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Defines clustered characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class ClusteringConfiguration {
   public static final AttributeDefinition<CacheMode> CACHE_MODE = AttributeDefinition.builder("cacheMode",  CacheMode.LOCAL).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusteringConfiguration.class, CACHE_MODE);
   }

   private final Attribute<CacheMode> cacheMode;
   private final AsyncConfiguration asyncConfiguration;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final SyncConfiguration syncConfiguration;
   private final PartitionHandlingConfiguration partitionHandlingConfiguration;
   private final AttributeSet attributes;

   ClusteringConfiguration(AttributeSet attributes, AsyncConfiguration asyncConfiguration, HashConfiguration hashConfiguration,
         L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration, SyncConfiguration syncConfiguration,
         PartitionHandlingConfiguration partitionHandlingStrategy) {
      this.attributes = attributes.checkProtection();
      this.cacheMode = attributes.attribute(CACHE_MODE);
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
      return cacheMode.get();
   }

   /**
    * Configures cluster's behaviour in the presence of partitions or node failures.
    */
   public PartitionHandlingConfiguration partitionHandling() {
      return partitionHandlingConfiguration;
   }

   public String cacheModeString() {

      return cacheMode() == null ? "none" : cacheMode().toString();
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

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "ClusteringConfiguration [asyncConfiguration=" + asyncConfiguration + ", hashConfiguration="
            + hashConfiguration + ", l1Configuration=" + l1Configuration + ", stateTransferConfiguration="
            + stateTransferConfiguration + ", syncConfiguration=" + syncConfiguration
            + ", partitionHandlingConfiguration=" + partitionHandlingConfiguration + ", attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ClusteringConfiguration other = (ClusteringConfiguration) obj;
      if (asyncConfiguration == null) {
         if (other.asyncConfiguration != null)
            return false;
      } else if (!asyncConfiguration.equals(other.asyncConfiguration))
         return false;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      if (hashConfiguration == null) {
         if (other.hashConfiguration != null)
            return false;
      } else if (!hashConfiguration.equals(other.hashConfiguration))
         return false;
      if (l1Configuration == null) {
         if (other.l1Configuration != null)
            return false;
      } else if (!l1Configuration.equals(other.l1Configuration))
         return false;
      if (partitionHandlingConfiguration == null) {
         if (other.partitionHandlingConfiguration != null)
            return false;
      } else if (!partitionHandlingConfiguration.equals(other.partitionHandlingConfiguration))
         return false;
      if (stateTransferConfiguration == null) {
         if (other.stateTransferConfiguration != null)
            return false;
      } else if (!stateTransferConfiguration.equals(other.stateTransferConfiguration))
         return false;
      if (syncConfiguration == null) {
         if (other.syncConfiguration != null)
            return false;
      } else if (!syncConfiguration.equals(other.syncConfiguration))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((asyncConfiguration == null) ? 0 : asyncConfiguration.hashCode());
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      result = prime * result + ((hashConfiguration == null) ? 0 : hashConfiguration.hashCode());
      result = prime * result + ((l1Configuration == null) ? 0 : l1Configuration.hashCode());
      result = prime * result
            + ((partitionHandlingConfiguration == null) ? 0 : partitionHandlingConfiguration.hashCode());
      result = prime * result + ((stateTransferConfiguration == null) ? 0 : stateTransferConfiguration.hashCode());
      result = prime * result + ((syncConfiguration == null) ? 0 : syncConfiguration.hashCode());
      return result;
   }

}
