package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
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

   private ConsistentHashFactory consistentHashFactory;
   private Hash hash = new MurmurHash3();
   private int numOwners = 2;
   // With the default consistent hash factory, this default gives us an even spread for clusters
   // up to 6 members and the difference between nodes stays under 20% up to 12 members.
   private int numSegments = 60;
   private float capacityFactor = 1;

   private final GroupsConfigurationBuilder groupsConfigurationBuilder;

   HashConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
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
   public HashConfigurationBuilder consistentHashFactory(ConsistentHashFactory consistentHashFactory) {
      this.consistentHashFactory = consistentHashFactory;
      return this;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public HashConfigurationBuilder numOwners(int numOwners) {
      if (numOwners < 1) throw new IllegalArgumentException("numOwners cannot be less than 1");
      this.numOwners = numOwners;
      return this;
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
      this.numSegments = numSegments;
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
    */
   public HashConfigurationBuilder hash(Hash hash) {
      this.hash = hash;
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
      this.capacityFactor = capacityFactor;
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
      return new HashConfiguration(consistentHashFactory, hash, numOwners, numSegments, capacityFactor,
            groupsConfigurationBuilder.create(), stateTransfer().create());
   }

   @Override
   public HashConfigurationBuilder read(HashConfiguration template) {
      this.consistentHashFactory = template.consistentHashFactory();
      this.hash = template.hash();
      this.numOwners = template.numOwners();
      this.numSegments = template.numSegments();
      this.groupsConfigurationBuilder.read(template.groups());
      this.capacityFactor = template.capacityFactor();
      return this;
   }

   @Override
   public String toString() {
      return "HashConfigurationBuilder{" +
            "consistentHashFactory=" + consistentHashFactory +
            ", hash=" + hash +
            ", numOwners=" + numOwners +
            ", numSegments=" + numSegments +
            ", groups=" + groupsConfigurationBuilder +
            '}';
   }
}
