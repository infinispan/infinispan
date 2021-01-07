package org.infinispan.configuration.cache;


import static org.infinispan.configuration.cache.IndexMergeConfiguration.CALIBRATE_BY_DELETES;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.FACTOR;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MAX_ENTRIES;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MAX_FORCED_SIZE;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MAX_SIZE;
import static org.infinispan.configuration.cache.IndexMergeConfiguration.MIN_SIZE;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 12.0
 */
public class IndexMergeConfigurationBuilder extends AbstractIndexingConfigurationChildBuilder
      implements Builder<IndexMergeConfiguration>, ConfigurationBuilderInfo {

   private static final String KEY_PREFIX = "hibernate.search.backend.io.merge";
   private static final String MAX_ENTRIES_KEY = KEY_PREFIX + ".max_docs";
   private static final String FACTOR_KEY = KEY_PREFIX + ".factor";
   private static final String MIN_SIZE_KEY = KEY_PREFIX + ".min_size";
   private static final String MAX_SIZE_KEY = KEY_PREFIX + ".max_size";
   private static final String MAX_FORCED_SIZE_KEY = KEY_PREFIX + ".max_forced_size";
   private static final String CALIBRATE_BY_DELETES_KEY = KEY_PREFIX + ".calibrate_by_deletes";

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

   Map<String, Object> asInternalProperties() {
      Map<String, Object> props = new HashMap<>();
      if (!maxEntries.isNull()) {
         props.put(MAX_ENTRIES_KEY, maxEntries());
      }
      if (!minSize.isNull()) {
         props.put(MIN_SIZE_KEY, minSize());
      }
      if (!maxSize.isNull()) {
         props.put(MAX_SIZE_KEY, maxSize());
      }
      if (!factor.isNull()) {
         props.put(FACTOR_KEY, factor());
      }
      if (!maxForceSize.isNull()) {
         props.put(MAX_FORCED_SIZE_KEY, maxForcedSize());
      }
      if (!calibrateByDeletes.isNull()) {
         props.put(CALIBRATE_BY_DELETES_KEY, isCalibrateByDeletes());
      }
      return props;
   }

   @Override
   public ElementDefinition<IndexMergeConfiguration> getElementDefinition() {
      return IndexMergeConfiguration.ELEMENT_DEFINITION;
   }

   public IndexMergeConfigurationBuilder maxEntries(int value) {
      maxEntries.set(value);
      return this;
   }

   public Integer maxEntries() {
      return maxEntries.get();
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

   public Integer maxSize() {
      return maxSize.get();
   }

   public IndexMergeConfigurationBuilder maxForcedSize(int value) {
      maxForceSize.set(value);
      return this;
   }

   public Integer maxForcedSize() {
      return maxForceSize.get();
   }

   public IndexMergeConfigurationBuilder calibrateByDeletes(boolean value) {
      calibrateByDeletes.set(value);
      return this;
   }

   public Boolean isCalibrateByDeletes() {
      return calibrateByDeletes.get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

}
