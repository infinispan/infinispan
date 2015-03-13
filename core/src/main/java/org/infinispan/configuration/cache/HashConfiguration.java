package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 *
 * @author pmuir
 */
public class HashConfiguration {
   static final AttributeDefinition<ConsistentHashFactory> CONSISTENT_HASH_FACTORY  = AttributeDefinition.builder("consistentHashFactory", null, ConsistentHashFactory.class).immutable().build();
   static final AttributeDefinition<Hash> HASH = AttributeDefinition.builder("hash", (Hash)MurmurHash3.getInstance()).immutable().build();
   static final AttributeDefinition<Integer> NUM_OWNERS = AttributeDefinition.builder("numOwners" , 2).immutable().build();
   // With the default consistent hash factory, this default gives us an even spread for clusters
   // up to 6 members and the difference between nodes stays under 20% up to 12 members.
   static final AttributeDefinition<Integer> NUM_SEGMENTS = AttributeDefinition.builder("numSegments" , 60).immutable().build();
   static final AttributeDefinition<Float> CAPACITY_FACTOR= AttributeDefinition.builder("capacityFactor", 1.0f).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(HashConfiguration.class, CONSISTENT_HASH_FACTORY, HASH, NUM_OWNERS, NUM_SEGMENTS, CAPACITY_FACTOR);
   }

   private final GroupsConfiguration groupsConfiguration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final AttributeSet attributes;

   HashConfiguration(AttributeSet attributes, GroupsConfiguration groupsConfiguration,
                     StateTransferConfiguration stateTransferConfiguration) {
      attributes.checkProtection();
      this.attributes = attributes;
      this.groupsConfiguration = groupsConfiguration;
      this.stateTransferConfiguration = stateTransferConfiguration;
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
       return attributes.attribute(CONSISTENT_HASH_FACTORY).asObject(ConsistentHashFactory.class);
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator.
    * Typically one of the the many default {@link org.infinispan.distribution.ch.ConsistentHash}
    * implementations shipped.
    */
   public Hash hash() {
      return attributes.attribute(HASH).asObject(Hash.class);
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public int numOwners() {
      return attributes.attribute(NUM_OWNERS).asInteger();
   }

   /**
    * @deprecated No longer used since 5.2, replaced by {@link #numSegments()} (which works like a
    *    {@code numVirtualNodes} value for the entire cluster).
    */
   @Deprecated
   public int numVirtualNodes() {
      return 1;
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
      return attributes.attribute(NUM_SEGMENTS).asInteger();
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
      return attributes.attribute(CAPACITY_FACTOR).asFloat();
   }

   /**
    * Configuration for various grouper definitions. See the user guide for more information.
    */
   public GroupsConfiguration groups() {
      return groupsConfiguration;
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return attributes.toString();
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
      if (groupsConfiguration == null) {
         if (other.groupsConfiguration != null)
            return false;
      } else if (!groupsConfiguration.equals(other.groupsConfiguration))
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
      result = prime * result + ((groupsConfiguration == null) ? 0 : groupsConfiguration.hashCode());
      result = prime * result + ((stateTransferConfiguration == null) ? 0 : stateTransferConfiguration.hashCode());
      return result;
   }
}
