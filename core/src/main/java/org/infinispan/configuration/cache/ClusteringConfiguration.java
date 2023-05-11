package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;


/**
 * Defines clustered characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class ClusteringConfiguration extends ConfigurationElement<ClusteringConfiguration> {

   public static final AttributeDefinition<CacheMode> CACHE_MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MODE, CacheMode.LOCAL).immutable().build();
   public static final AttributeDefinition<Long> REMOTE_TIMEOUT =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REMOTE_TIMEOUT, TimeUnit.SECONDS.toMillis(15)).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusteringConfiguration.class, CACHE_MODE, REMOTE_TIMEOUT);
   }

   private final Attribute<CacheMode> cacheMode;
   private final Attribute<Long> remoteTimeout;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final PartitionHandlingConfiguration partitionHandlingConfiguration;

   ClusteringConfiguration(AttributeSet attributes, HashConfiguration hashConfiguration,
                           L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration,
                           PartitionHandlingConfiguration partitionHandlingStrategy) {
      super(Element.CLUSTERING, attributes, hashConfiguration, l1Configuration, stateTransferConfiguration, partitionHandlingStrategy);
      this.cacheMode = attributes.attribute(CACHE_MODE);
      this.remoteTimeout = attributes.attribute(REMOTE_TIMEOUT);
      this.hashConfiguration = hashConfiguration;
      this.l1Configuration = l1Configuration;
      this.stateTransferConfiguration = stateTransferConfiguration;
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
      return remoteTimeout.get();
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public void remoteTimeout(long timeoutMillis) {
      remoteTimeout.set(timeoutMillis);
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
}
