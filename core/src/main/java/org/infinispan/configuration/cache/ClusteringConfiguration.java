package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.parsing.Element;


/**
 * Defines clustered characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class ClusteringConfiguration extends ConfigurationElement<ClusteringConfiguration> {
   public static final AttributeDefinition<CacheType> CACHE_TYPE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TYPE, CacheType.LOCAL).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> CACHE_SYNC = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MODE, true, Boolean.class).immutable().autoPersist(false).build();
   public static final AttributeDefinition<TimeQuantity> REMOTE_TIMEOUT =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REMOTE_TIMEOUT, TimeQuantity.valueOf("15s")).parser(TimeQuantity.PARSER).build();
   public static final AttributeDefinition<Boolean> REPLICATE_PUTS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REPLICATE_PUTS, true, Boolean.class).immutable().autoPersist(false).build();


   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusteringConfiguration.class, CACHE_TYPE, CACHE_SYNC, REMOTE_TIMEOUT, REPLICATE_PUTS);
   }

   private final CacheMode cacheMode;
   private final Attribute<TimeQuantity> remoteTimeout;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final PartitionHandlingConfiguration partitionHandlingConfiguration;
   private final Attribute<Boolean> replicatePuts;

   ClusteringConfiguration(AttributeSet attributes, HashConfiguration hashConfiguration,
                           L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration,
                           PartitionHandlingConfiguration partitionHandlingStrategy) {
      super(Element.CLUSTERING, attributes, hashConfiguration, l1Configuration, stateTransferConfiguration, partitionHandlingStrategy);
      this.cacheMode = CacheMode.of(attributes.attribute(CACHE_TYPE).get(), attributes.attribute(CACHE_SYNC).get());
      this.remoteTimeout = attributes.attribute(REMOTE_TIMEOUT);
      this.hashConfiguration = hashConfiguration;
      this.l1Configuration = l1Configuration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.partitionHandlingConfiguration  = partitionHandlingStrategy;
      this.replicatePuts = attributes.attribute(REPLICATE_PUTS);
   }

   /**
    * Cache mode. See {@link CacheMode} for information on the various cache modes available.
    */
   public CacheMode cacheMode() {
      return cacheMode;
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public long remoteTimeout() {
      return remoteTimeout.get().longValue();
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public void remoteTimeout(long timeoutMillis) {
      remoteTimeout.set(TimeQuantity.valueOf(timeoutMillis));
   }

   public void remoteTimeout(String timeout) {
      remoteTimeout.set(TimeQuantity.valueOf(timeout));
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

   public StateTransferConfiguration stateTransfer() {
      return stateTransferConfiguration;
   }

    /**
     * If true, puts are replicated to all nodes in the cluster. If false, puts are not replicated.
     */
    public boolean replicatePuts() {
       return replicatePuts.get();
    }
}
