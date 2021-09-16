package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.IndexMergeConfiguration.CALIBRATE_BY_DELETES;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.FACTOR;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MAX_ENTRIES;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MAX_FORCED_SIZE;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MAX_SIZE;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MIN_SIZE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 12.0
 */
public class IndexMergeConfigurationBuilder extends AbstractIndexingConfigurationChildBuilder
      implements Builder<IndexMergeConfiguration> {

   private final AttributeSet attributes;
   private final Attribute<Integer> maxEntries;
   private final Attribute<Integer> factor;
   private final Attribute<Integer> minSize;
   private final Attribute<Integer> maxSize;
   private final Attribute<Integer> maxForceSize;
   private final Attribute<Boolean> calibrateByDeletes;

   IndexMergeConfigurationBuilder(IndexingConfigurationBuilder builder) {
      super(builder);
      this.attributes = IndexMergeConfiguration.attributeDefinitionSet();
      this.maxEntries = attributes.attribute(MAX_ENTRIES);
      this.factor = attributes.attribute(FACTOR);
      this.minSize = attributes.attribute(MIN_SIZE);
      this.maxSize = attributes.attribute(MAX_SIZE);
      this.maxForceSize = attributes.attribute(MAX_FORCED_SIZE);
      this.calibrateByDeletes = attributes.attribute(CALIBRATE_BY_DELETES);
   }

   public IndexMergeConfigurationBuilder maxEntries(int value) {
      maxEntries.set(value);
      return this;
   }

   public IndexMergeConfigurationBuilder factor(int value) {
      factor.set(value);
      return this;
   }

   public Integer factor() {
      return factor.get();
   }

   public IndexMergeConfigurationBuilder minSize(int value) {
      minSize.set(value);
      return this;
   }

   public Integer minSize() {
      return minSize.get();
   }

   public IndexMergeConfigurationBuilder maxSize(int value) {
      maxSize.set(value);
      return this;
   }

   public IndexMergeConfigurationBuilder maxForcedSize(int value) {
      maxForceSize.set(value);
      return this;
   }

   public IndexMergeConfigurationBuilder calibrateByDeletes(boolean value) {
      calibrateByDeletes.set(value);
      return this;
   }

   @Override
   public IndexMergeConfiguration create() {
      return new IndexMergeConfiguration(attributes.protect());
   }

   @Override
   public IndexMergeConfigurationBuilder read(IndexMergeConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "IndexMergeConfiguration{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

}
