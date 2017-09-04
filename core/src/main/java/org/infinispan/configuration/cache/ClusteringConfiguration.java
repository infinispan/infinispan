package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;


/**
 * Defines clustered characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class ClusteringConfiguration implements Matchable<ClusteringConfiguration> {
   public static final AttributeDefinition<CacheMode> CACHE_MODE = AttributeDefinition.builder("cacheMode",  CacheMode.LOCAL).immutable().build();
   public static final AttributeDefinition<Long> REMOTE_TIMEOUT =
         AttributeDefinition.builder("remoteTimeout", TimeUnit.SECONDS.toMillis(15)).build();
   public static final AttributeDefinition<Integer> INVALIDATION_BATCH_SIZE = AttributeDefinition.builder("invalidationBatchSize",  128).immutable().build();
   public static final AttributeDefinition<BiasAcquisition> BIAS_ACQUISITION = AttributeDefinition.builder("biasAcquisition", BiasAcquisition.ON_WRITE).immutable().build();
   public static final AttributeDefinition<Long> BIAS_LIFESPAN = AttributeDefinition.builder("biasLifespan", TimeUnit.MINUTES.toMillis(5)).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusteringConfiguration.class, CACHE_MODE, REMOTE_TIMEOUT, INVALIDATION_BATCH_SIZE, BIAS_ACQUISITION, BIAS_LIFESPAN);
   }

   private final Attribute<CacheMode> cacheMode;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final SyncConfiguration syncConfiguration;
   private final PartitionHandlingConfiguration partitionHandlingConfiguration;
   private final AttributeSet attributes;

   ClusteringConfiguration(AttributeSet attributes, HashConfiguration hashConfiguration,
         L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration, SyncConfiguration syncConfiguration,
         PartitionHandlingConfiguration partitionHandlingStrategy) {
      this.attributes = attributes.checkProtection();
      this.cacheMode = attributes.attribute(CACHE_MODE);
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
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public long remoteTimeout() {
      return syncConfiguration.replTimeout();
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public void remoteTimeout(long timeoutMillis) {
      syncConfiguration.replTimeout(timeoutMillis);
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
    * For scattered cache, the threshold after which batched invalidations are sent
    */
   public int invalidationBatchSize() {
      return attributes.attribute(INVALIDATION_BATCH_SIZE).get();
   }

   /**
    * For scattered cache, specifies if the nodes is allowed to cache the entry, serving reads locally.
    */
   public BiasAcquisition biasAcquisition() {
      return attributes.attribute(BIAS_ACQUISITION).get();
   }

   /**
    * For scattered cache, specifies how long is the node allowed to read the cached entry locally.
    */
   public long biasLifespan() {
      return attributes.attribute(BIAS_LIFESPAN).get();
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
    *
    * @deprecated Since 9.0, the {@code replTimeout} attribute is now in {@link ClusteringConfiguration}.
    */
   @Deprecated
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
      return "ClusteringConfiguration [hashConfiguration=" + hashConfiguration +
            ", l1Configuration=" + l1Configuration +
            ", stateTransferConfiguration=" + stateTransferConfiguration +
            ", syncConfiguration=" + syncConfiguration
            + ", partitionHandlingConfiguration=" + partitionHandlingConfiguration +
            ", attributes=" + attributes + "]";
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
