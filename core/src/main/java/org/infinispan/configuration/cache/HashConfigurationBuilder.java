package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.HashConfiguration.CAPACITY_FACTOR;
import static org.infinispan.configuration.cache.HashConfiguration.CONSISTENT_HASH_FACTORY;
import static org.infinispan.configuration.cache.HashConfiguration.HASH;
import static org.infinispan.configuration.cache.HashConfiguration.KEY_PARTITIONER;
import static org.infinispan.configuration.cache.HashConfiguration.NUM_OWNERS;
import static org.infinispan.configuration.cache.HashConfiguration.NUM_SEGMENTS;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.hash.Hash;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 *
 * @author pmuir
 *
 */
public class HashConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<HashConfiguration> {
   private static final Log log = LogFactory.getLog(HashConfigurationBuilder.class);

   private final AttributeSet attributes;
   private final GroupsConfigurationBuilder groupsConfigurationBuilder;

   HashConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      this.attributes = HashConfiguration.attributeDefinitionSet();
      this.groupsConfigurationBuilder = new GroupsConfigurationBuilder(builder);
   }

   /**
    * @deprecated Since 5.2, replaced by {@link #consistentHashFactory(ConsistentHashFactory)}.
    */
   @Deprecated
   public HashConfigurationBuilder consistentHash(ConsistentHash consistentHash) {
      log.consistentHashDeprecated();
      return this;
   }

   /**
    * The consistent hash factory in use.
    */
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
    * @deprecated No longer used since 5.2, replaced by {@link #numSegments(int)} (which works like a
    *    {@code numVirtualNodes} value for the entire cluster).
    */
   @Deprecated
   public HashConfigurationBuilder numVirtualNodes(int numVirtualNodes) {
      log.hashNumVirtualNodesDeprecated();
      return this;
   }

   /**
    * Controls the total number of hash space segments (per cluster).
    *
    * <p>A hash space segment is the granularity for key distribution in the cluster: a node can own
    * (or primary-own) one or more full segments, but not a fraction of a segment. As such, larger
    * {@code numSegments} values will mean a more even distribution of keys between nodes.
    * <p>On the other hand, the memory/bandwidth usage of the new consistent hash grows linearly with
    * {@code numSegments}. So we recommend keeping {@code numSegments <= 10 * clusterSize}.
    *
    * @param numSegments the number of hash space segments. Must be strictly positive.
    */
   public HashConfigurationBuilder numSegments(int numSegments) {
      if (numSegments < 1) throw new IllegalArgumentException("numSegments cannot be less than 1");
      attributes.attribute(NUM_SEGMENTS).set(numSegments);
      return this;
   }

   /**
    * Enable rebalancing and rehashing, which will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   @Deprecated
   public HashConfigurationBuilder rehashEnabled() {
      stateTransfer().fetchInMemoryState(true);
      return this;
   }

   /**
    * Enable rebalancing and rehashing, which will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   @Deprecated
   public HashConfigurationBuilder rehashEnabled(boolean enabled) {
      stateTransfer().fetchInMemoryState(enabled);
      return this;
   }

   /**
    * Disable rebalancing and rehashing, which would have taken place when a new node joins the
    * cluster or a node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   @Deprecated
   public HashConfigurationBuilder rehashDisabled() {
      stateTransfer().fetchInMemoryState(false);
      return this;
   }

   /**
    * Rehashing timeout
    * @deprecated Use {@link StateTransferConfigurationBuilder#timeout(long)} instead.
    */
   @Deprecated
   public HashConfigurationBuilder rehashRpcTimeout(long rehashRpcTimeout) {
      stateTransfer().timeout(rehashRpcTimeout);
      return this;
   }

   /**
    * @deprecated No longer used.
    */
   @Deprecated
   public HashConfigurationBuilder rehashWait(long rehashWait) {
      return this;
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator. Typically
    * used in conjunction with the many default
    * {@link org.infinispan.distribution.ch.ConsistentHash} implementations shipped.
    *
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    * @deprecated Since 8.2, use {@link #keyPartitioner(KeyPartitioner)} instead.
    */
   @Deprecated
   public HashConfigurationBuilder hash(Hash hash) {
      attributes.attribute(HASH).set(hash);
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
    * The default implementation {@code org.infinispan.distribution.ch.impl.HashFunctionPartitioner},
    * uses the hash function configured via {@link #hash(Hash)}. Future versions may ignore the hash function.
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
      // TODO stateTransfer().create() will create a duplicate StateTransferConfiguration instance. That's ok as long as none of the stateTransfer settings are modifiable at runtime.
      return new HashConfiguration(attributes.protect(),
            groupsConfigurationBuilder.create(), stateTransfer().create());
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
}
