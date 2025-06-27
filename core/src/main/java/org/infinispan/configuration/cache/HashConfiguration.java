package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;

/**
 * Allows fine-tuning of rehashing characteristics. Must be used only with 'distributed' cache mode.
 *
 * @author pmuir
 */
public class HashConfiguration extends ConfigurationElement<HashConfiguration> {
   public static final AttributeDefinition<Integer> NUM_OWNERS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.OWNERS, 2).immutable().build();
   // Because it assigns owners randomly, SyncConsistentHashFactory doesn't work very well with a low number
   // of segments. (With DefaultConsistentHashFactory, 60 segments was ok up to 6 nodes.)
   public static final AttributeDefinition<Integer> NUM_SEGMENTS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SEGMENTS, 256).immutable().build();
   public static final AttributeDefinition<Float> CAPACITY_FACTOR = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CAPACITY_FACTOR, 1.0f).immutable().global(false).build();
   public static final AttributeDefinition<KeyPartitioner> KEY_PARTITIONER = AttributeDefinition
         .builder(org.infinispan.configuration.parsing.Attribute.KEY_PARTITIONER, null, KeyPartitioner.class)
         .copier(original -> {
            KeyPartitioner copy = Util.getInstance(original.getClass());
            copy.init(original);
            return copy;
         })
         .initializer(HashFunctionPartitioner::new)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME)
         .immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(HashConfiguration.class, NUM_OWNERS,
            NUM_SEGMENTS, CAPACITY_FACTOR, KEY_PARTITIONER);
   }

   private final Attribute<Integer> numOwners;
   private final Attribute<Integer> numSegments;
   private final Attribute<Float> capacityFactor;

   private final GroupsConfiguration groupsConfiguration;

   HashConfiguration(AttributeSet attributes, GroupsConfiguration groupsConfiguration) {
      super(Element.HASH, attributes);
      this.groupsConfiguration = groupsConfiguration;
      numOwners = attributes.attribute(NUM_OWNERS);
      numSegments = attributes.attribute(NUM_SEGMENTS);
      capacityFactor = attributes.attribute(CAPACITY_FACTOR);
      attributes.attribute(KEY_PARTITIONER).get().init(this);
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
    * (or primary-own) one or more full segments, but not a fraction of a segment.
    * As such, very small {@code numSegments} values (&lt; 10 segments per node) will make
    * the distribution of keys between nodes more uneven.</p>
    * <p>The recommended value is 20 * the expected cluster size.</p>
    * <p>Note: The value returned by {@link ConsistentHash#getNumSegments()} may be different,
    * e.g. rounded up to a power of 2.</p>
    */
   public int numSegments() {
      return numSegments.get();
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
      return attributes.attribute(KEY_PARTITIONER).get();
   }

   /**
    * Configuration for various grouper definitions. See the user guide for more information.
    */
   public GroupsConfiguration groups() {
      return groupsConfiguration;
   }
}
