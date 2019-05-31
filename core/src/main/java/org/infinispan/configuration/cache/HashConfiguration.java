package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ClassAttributeSerializer;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.attributes.SimpleInstanceAttributeCopier;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 *
 * @author pmuir
 */
public class HashConfiguration implements Matchable<HashConfiguration>, ConfigurationInfo {
   public static final AttributeDefinition<ConsistentHashFactory> CONSISTENT_HASH_FACTORY = AttributeDefinition.builder("consistentHashFactory", null, ConsistentHashFactory.class).serializer(ClassAttributeSerializer.INSTANCE).immutable().build();
   public static final AttributeDefinition<Integer> NUM_OWNERS = AttributeDefinition.builder("numOwners" , 2).xmlName("owners").immutable().build();
   // Because it assigns owners randomly, SyncConsistentHashFactory doesn't work very well with a low number
   // of segments. (With DefaultConsistentHashFactory, 60 segments was ok up to 6 nodes.)
   public static final AttributeDefinition<Integer> NUM_SEGMENTS = AttributeDefinition.builder("numSegments", 256).xmlName("segments").immutable().build();
   public static final AttributeDefinition<Float> CAPACITY_FACTOR= AttributeDefinition.builder("capacityFactor", 1.0f).immutable().global(false).xmlName("capacity").build();
   public static final AttributeDefinition<KeyPartitioner> KEY_PARTITIONER = AttributeDefinition
         .builder("keyPartitioner", new HashFunctionPartitioner(), KeyPartitioner.class)
         .copier(SimpleInstanceAttributeCopier.INSTANCE)
         .serializer(ClassAttributeSerializer.INSTANCE)
         .immutable().build();

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.HASH.getLocalName(), false);

   private final List<ConfigurationInfo> elements;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(HashConfiguration.class, CONSISTENT_HASH_FACTORY, NUM_OWNERS,
            NUM_SEGMENTS, CAPACITY_FACTOR, KEY_PARTITIONER);
   }

   private final Attribute<ConsistentHashFactory> consistentHashFactory;
   private final Attribute<Integer> numOwners;
   private final Attribute<Integer> numSegments;
   private final Attribute<Float> capacityFactor;
   private final Attribute<KeyPartitioner> keyPartitioner;

   private final GroupsConfiguration groupsConfiguration;
   private final AttributeSet attributes;

   HashConfiguration(AttributeSet attributes, GroupsConfiguration groupsConfiguration) {
      this.attributes = attributes.checkProtection();
      this.groupsConfiguration = groupsConfiguration;
      consistentHashFactory = attributes.attribute(CONSISTENT_HASH_FACTORY);
      numOwners = attributes.attribute(NUM_OWNERS);
      numSegments = attributes.attribute(NUM_SEGMENTS);
      capacityFactor = attributes.attribute(CAPACITY_FACTOR);
      keyPartitioner = attributes.attribute(KEY_PARTITIONER);
      elements = Collections.singletonList(groupsConfiguration);
   }

   /**
    * The consistent hash factory in use.
    */
   public ConsistentHashFactory<?> consistentHashFactory() {
       return consistentHashFactory.get();
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
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
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
