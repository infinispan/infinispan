package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Attribute.MODE;
import static org.infinispan.configuration.parsing.Element.CLUSTERING;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;


/**
 * Defines clustered characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class ClusteringConfiguration implements Matchable<ClusteringConfiguration>, ConfigurationInfo {

   public static final AttributeDefinition<CacheMode> CACHE_MODE = AttributeDefinition.builder("cacheMode", CacheMode.LOCAL).serializer(new AttributeSerializer<CacheMode, ClusteringConfiguration, ConfigurationBuilderInfo>() {
      @Override
      public String getSerializationName(Attribute<CacheMode> attribute, ClusteringConfiguration element) {
         if (element.cacheMode().isClustered()) {
            return MODE.getLocalName();
         }
         return null;
      }

      @Override
      public Object getSerializationValue(Attribute<CacheMode> attribute, ClusteringConfiguration element) {
         CacheMode cacheMode = attribute.get();
         if (cacheMode.isClustered()) return cacheMode.toString().split("_")[1];
         return null;
      }

      @Override
      public boolean canRead(String enclosing, String nestingName, String nestedName, AttributeDefinition attributeDefinition) {
         return nestedName != null && nestedName.equals(MODE.getLocalName());
      }

      @Override
      public Object readAttributeValue(String enclosing, String nesting, AttributeDefinition attributeDefinition, Object value, ConfigurationBuilderInfo builderInfo) {
         return CacheMode.fromParts(enclosing.substring(0, enclosing.indexOf("-")), value.toString());
      }

   }).immutable().build();
   public static final AttributeDefinition<Long> REMOTE_TIMEOUT =
         AttributeDefinition.builder("remoteTimeout", TimeUnit.SECONDS.toMillis(15)).build();
   public static final AttributeDefinition<Integer> INVALIDATION_BATCH_SIZE = AttributeDefinition.builder("invalidationBatchSize",  128).immutable().build();
   public static final AttributeDefinition<BiasAcquisition> BIAS_ACQUISITION = AttributeDefinition.builder("biasAcquisition", BiasAcquisition.ON_WRITE).immutable().build();
   public static final AttributeDefinition<Long> BIAS_LIFESPAN = AttributeDefinition.builder("biasLifespan", TimeUnit.MINUTES.toMillis(5)).immutable().build();
   private final List<ConfigurationInfo> elements;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusteringConfiguration.class, CACHE_MODE, REMOTE_TIMEOUT, INVALIDATION_BATCH_SIZE, BIAS_ACQUISITION, BIAS_LIFESPAN);
   }

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(CLUSTERING.getLocalName(), false);

   private final Attribute<CacheMode> cacheMode;
   private final Attribute<Long> remoteTimeout;
   private final Attribute<Integer> invalidationBatchSize;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final PartitionHandlingConfiguration partitionHandlingConfiguration;
   private final AttributeSet attributes;

   ClusteringConfiguration(AttributeSet attributes, HashConfiguration hashConfiguration,
                           L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration,
                           PartitionHandlingConfiguration partitionHandlingStrategy) {
      this.attributes = attributes.checkProtection();
      this.cacheMode = attributes.attribute(CACHE_MODE);
      this.remoteTimeout = attributes.attribute(REMOTE_TIMEOUT);
      this.invalidationBatchSize = attributes.attribute(INVALIDATION_BATCH_SIZE);
      this.hashConfiguration = hashConfiguration;
      this.l1Configuration = l1Configuration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.partitionHandlingConfiguration  = partitionHandlingStrategy;
      this.elements = Arrays.asList(hashConfiguration, l1Configuration, stateTransferConfiguration, partitionHandlingStrategy);
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
      return attributes.attribute(REMOTE_TIMEOUT).get();
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public void remoteTimeout(long timeoutMillis) {
      attributes.attribute(REMOTE_TIMEOUT).set(timeoutMillis);
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
      return invalidationBatchSize.get();
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

   public StateTransferConfiguration stateTransfer() {
      return stateTransferConfiguration;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   @Override
   public boolean matches(ClusteringConfiguration other) {
      return (attributes.matches(other.attributes) &&
            hashConfiguration.matches(other.hashConfiguration) &&
            l1Configuration.matches(other.l1Configuration) &&
            partitionHandlingConfiguration.matches(other.partitionHandlingConfiguration) &&
            stateTransferConfiguration.matches(other.stateTransferConfiguration));
   }

   @Override
   public String toString() {
      return "ClusteringConfiguration [hashConfiguration=" + hashConfiguration +
            ", l1Configuration=" + l1Configuration +
            ", stateTransferConfiguration=" + stateTransferConfiguration +
            ", partitionHandlingConfiguration=" + partitionHandlingConfiguration +
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
      return result;
   }

}
