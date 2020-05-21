package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.HashConfiguration.CAPACITY_FACTOR;
import static org.infinispan.configuration.cache.HashConfiguration.CONSISTENT_HASH_FACTORY;
import static org.infinispan.configuration.cache.HashConfiguration.KEY_PARTITIONER;
import static org.infinispan.configuration.cache.HashConfiguration.NUM_OWNERS;
import static org.infinispan.configuration.cache.HashConfiguration.NUM_SEGMENTS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 *
 * @author pmuir
 */
public class HashConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<HashConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;
   private final GroupsConfigurationBuilder groupsConfigurationBuilder;
   private final List<ConfigurationBuilderInfo> subElements = new ArrayList<>();

   HashConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      this.attributes = HashConfiguration.attributeDefinitionSet();
      this.groupsConfigurationBuilder = new GroupsConfigurationBuilder(builder);
      subElements.add(groupsConfigurationBuilder);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return HashConfiguration.ELEMENT_DEFINITION;
   }

   /**
    * The consistent hash factory in use.
    * @deprecated Since 11.0. Will be removed in 14.0, the segment allocation will no longer be customizable.
    */
   @Deprecated
   public HashConfigurationBuilder consistentHashFactory(ConsistentHashFactory<? extends ConsistentHash> consistentHashFactory) {
      attributes.attribute(CONSISTENT_HASH_FACTORY).set(consistentHashFactory);
      return this;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public HashConfigurationBuilder numOwners(int numOwners) {
      if (numOwners < 1) throw new IllegalArgumentException("numOwners cannot be less than 1");
      attributes.attribute(NUM_OWNERS).set(numOwners);
      return this;
   }

   boolean isNumOwnersSet() {
      return attributes.attribute(NUM_OWNERS).isModified();
   }

   int numOwners() {
      return attributes.attribute(NUM_OWNERS).get();
   }

   /**
    * Controls the total number of hash space segments (per cluster).
    *
    * <p>A hash space segment is the granularity for key distribution in the cluster: a node can own
    * (or primary-own) one or more full segments, but not a fraction of a segment.
    * As such, very small {@code numSegments} values (&lt; 10 segments per node) will make
    * the distribution of keys between nodes more uneven.</p>
    * <p>The recommended value is 20 * the expected cluster size.</p>
    * <p>Note: The value returned by {@link ConsistentHash#getNumSegments()} may be different,
    * e.g. rounded up to a power of 2.</p>
    *
    * @param numSegments the number of hash space segments. Must be strictly positive.
    */
   public HashConfigurationBuilder numSegments(int numSegments) {
      if (numSegments < 1) throw new IllegalArgumentException("numSegments cannot be less than 1");
      attributes.attribute(NUM_SEGMENTS).set(numSegments);
      return this;
   }

   /**
    * Controls the proportion of entries that will reside on the local node, compared to the other nodes in the
    * cluster. This is just a suggestion, there is no guarantee that a node with a capacity factor of {@code 2} will
    * have twice as many entries as a node with a capacity factor of {@code 1}.
    * @param capacityFactor the capacity factor for the local node. Must be positive.
    */
   public HashConfigurationBuilder capacityFactor(float capacityFactor) {
      if (capacityFactor < 0) throw new IllegalArgumentException("capacityFactor must be positive");
      attributes.attribute(CAPACITY_FACTOR).set(capacityFactor);
      return this;
   }

   /**
    * Key partitioner, controlling the mapping of keys to hash segments.
    * <p>
    * The default implementation is {@code org.infinispan.distribution.ch.impl.HashFunctionPartitioner},
    * uses {@link org.infinispan.commons.hash.MurmurHash3}.
    *
    * @since 8.2
    */
   public HashConfigurationBuilder keyPartitioner(KeyPartitioner keyPartitioner) {
      attributes.attribute(KEY_PARTITIONER).set(keyPartitioner);
      return this;
   }

   public GroupsConfigurationBuilder groups() {
      return groupsConfigurationBuilder;
   }

   @Override
   public void validate() {
      groupsConfigurationBuilder.validate();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      groupsConfigurationBuilder.validate(globalConfig);
   }

   @Override
   public HashConfiguration create() {
      return new HashConfiguration(attributes.protect(), groupsConfigurationBuilder.create());
   }

   @Override
   public HashConfigurationBuilder read(HashConfiguration template) {
      this.attributes.read(template.attributes());
      this.groupsConfigurationBuilder.read(template.groups());
      return this;
   }

   @Override
   public String toString() {
      return "HashConfigurationBuilder [attributes=" + attributes + ", groupsConfigurationBuilder="
            + groupsConfigurationBuilder + "]";
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }
}
