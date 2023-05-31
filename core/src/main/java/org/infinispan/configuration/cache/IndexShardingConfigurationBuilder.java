package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.IndexShardingConfiguration.SHARDS;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 15.0
 */
public class IndexShardingConfigurationBuilder extends AbstractIndexingConfigurationChildBuilder
      implements Builder<IndexShardingConfiguration> {

   private final AttributeSet attributes;
   private final Attribute<Integer> shards;

   IndexShardingConfigurationBuilder(IndexingConfigurationBuilder builder) {
      super(builder);
      this.attributes = IndexShardingConfiguration.attributeDefinitionSet();
      this.shards = attributes.attribute(SHARDS);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public IndexShardingConfigurationBuilder shards(int value) {
      shards.set(value);
      return this;
   }

   @Override
   public IndexShardingConfiguration create() {
      return new IndexShardingConfiguration(attributes.protect());
   }

   @Override
   public IndexShardingConfigurationBuilder read(IndexShardingConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "IndexShardingConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }
}
