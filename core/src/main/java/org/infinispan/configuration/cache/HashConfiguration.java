package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.attributes.SimpleInstanceAttributeCopier;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 *
 * @author pmuir
 */
public class HashConfiguration implements Matchable<HashConfiguration> {
   public static final AttributeDefinition<ConsistentHashFactory> CONSISTENT_HASH_FACTORY  = AttributeDefinition.builder("consistentHashFactory", null, ConsistentHashFactory.class).immutable().build();
   public static final AttributeDefinition<Hash> HASH = AttributeDefinition.builder("hash", (Hash)MurmurHash3.getInstance()).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   public static final AttributeDefinition<Integer> NUM_OWNERS = AttributeDefinition.builder("numOwners" , 2).xmlName("owners").immutable().build();
   // Because it assigns owners randomly, SyncConsistentHashFactory doesn't work very well with a low number
   // of segments. (With DefaultConsistentHashFactory, 60 segments was ok up to 6 nodes.)
   public static final AttributeDefinition<Integer> NUM_SEGMENTS = AttributeDefinition.builder("numSegments", 256).xmlName("segments").immutable().build();
   public static final AttributeDefinition<Float> CAPACITY_FACTOR= AttributeDefinition.builder("capacityFactor", 1.0f).immutable().local(true).xmlName("capacity").build();
   public static final AttributeDefinition<KeyPartitioner> KEY_PARTITIONER = AttributeDefinition
         .builder("keyPartitioner", new HashFunctionPartitioner(), KeyPartitioner.class)
         .copier(SimpleInstanceAttributeCopier.INSTANCE).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(HashConfiguration.class, CONSISTENT_HASH_FACTORY, HASH, NUM_OWNERS,
            NUM_SEGMENTS, CAPACITY_FACTOR, KEY_PARTITIONER);
   }

   private final Attribute<ConsistentHashFactory> consistentHashFactory;
   private final Attribute<Hash> hash;
   private final Attribute<Integer> numOwners;
   private final Attribute<Integer> numSegments;
   private final Attribute<Float> capacityFactor;
   private final Attribute<KeyPartitioner> keyPartitioner;

   private final GroupsConfiguration groupsConfiguration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final AttributeSet attributes;

   HashConfiguration(AttributeSet attributes, GroupsConfiguration groupsConfiguration,
                     StateTransferConfiguration stateTransferConfiguration) {
      this.attributes = attributes.checkProtection();
      this.groupsConfiguration = groupsConfiguration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      consistentHashFactory = attributes.attribute(CONSISTENT_HASH_FACTORY);
      hash = attributes.attribute(HASH);
      numOwners = attributes.attribute(NUM_OWNERS);
      numSegments = attributes.attribute(NUM_SEGMENTS);
      capacityFactor = attributes.attribute(CAPACITY_FACTOR);
      keyPartitioner = attributes.attribute(KEY_PARTITIONER);
   }

   /**
    * @deprecated Since 5.2, replaced by {@link #consistentHashFactory()}.
    */
   @Deprecated
   public ConsistentHash consistentHash() {
      return null;
   }

   /**
    * The consistent hash factory in use.
    */
   public ConsistentHashFactory<?> consistentHashFactory() {
       return consistentHashFactory.get();
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator.
    * Typically one of the the many default {@link org.infinispan.distribution.ch.ConsistentHash}
    * implementations shipped.
    * @deprecated Since 8.2, use {@link #keyPartitioner()} instead.
    */
   @Deprecated
   public Hash hash() {
      return hash.get();
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public int numOwners() {
      return numOwners.get();
   }

   /**
    * Controls the total number of hash space segments (per cluster).
    *
    * <p>A hash space segment is the granularity for key distribution in the cluster: a node can own
    * (or primary-own) one or more full segments, but not a fraction of a segment. As such, larger
    * {@code numSegments} values will mean a more even distribution of keys between nodes.
    * <p>On the other hand, the memory/bandwidth usage of the new consistent hash grows linearly with
    * {@code numSegments}. So we recommend keeping {@code numSegments <= 10 * clusterSize}.
    */
   public int numSegments() {
      return numSegments.get();
   }

   /**
    * If false, no rebalancing or rehashing will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#fetchInMemoryState()} instead.
    */
   @Deprecated
   public boolean rehashEnabled() {
      return stateTransferConfiguration.fetchInMemoryState();
   }

   /**
    * Rehashing timeout
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout()} instead.
    */
   @Deprecated
   public long rehashRpcTimeout() {
      return stateTransferConfiguration.timeout();
   }

   /**
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout()} instead.
    */
   @Deprecated
   public long rehashWait() {
      return stateTransferConfiguration.timeout();
   }

   /**
    * Controls the proportion of entries that will reside on the local node, compared to the other nodes in the
    * cluster. This is just a suggestion, there is no guarantee that a node with a capacity factor of {@code 2} will
    * have twice as many entries as a node with a capacity factor of {@code 1}.
    */
   public float capacityFactor() {
      return capacityFactor.get();
   }

   public KeyPartitioner keyPartitioner() {
      return keyPartitioner.get();
   }

   /**
    * Configuration for various grouper definitions. See the user guide for more information.
    */
   public GroupsConfiguration groups() {
      return groupsConfiguration;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "HashConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      HashConfiguration other = (HashConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public boolean matches(HashConfiguration other) {
      return attributes.matches(other.attributes);
   }
}
