package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

public class IndexShardingConfiguration extends ConfigurationElement<IndexShardingConfiguration> {

   public static final AttributeDefinition<Integer> SHARDS =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SHARDS, 1, Integer.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexShardingConfiguration.class, SHARDS);
   }

   private final Attribute<Integer> shards;

   IndexShardingConfiguration(AttributeSet attributes) {
      super(Element.INDEX_SHARDING, attributes);
      this.shards = attributes.attribute(SHARDS);
   }

   public Integer getShards() {
      return shards.get();
   }
}
