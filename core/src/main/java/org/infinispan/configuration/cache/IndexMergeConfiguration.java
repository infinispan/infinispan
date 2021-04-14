package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 12.0
 */
public class IndexMergeConfiguration extends ConfigurationElement<IndexMergeConfiguration> {

   public static final AttributeDefinition<Integer> MAX_ENTRIES =
         AttributeDefinition.builder(Attribute.MAX_ENTRIES, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> FACTOR =
         AttributeDefinition.builder(Attribute.FACTOR, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> MIN_SIZE =
         AttributeDefinition.builder(Attribute.MIN_SIZE, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> MAX_SIZE =
         AttributeDefinition.builder(Attribute.MAX_SIZE, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> MAX_FORCED_SIZE =
         AttributeDefinition.builder(Attribute.MAX_FORCED_SIZE, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Boolean> CALIBRATE_BY_DELETES =
         AttributeDefinition.builder(Attribute.CALIBRATE_BY_DELETES, null, Boolean.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexMergeConfiguration.class, MAX_ENTRIES, FACTOR, MIN_SIZE, MAX_SIZE, MAX_FORCED_SIZE,
            CALIBRATE_BY_DELETES);
   }

   IndexMergeConfiguration(AttributeSet attributes) {
      super(Element.INDEX_MERGE, attributes);
   }

   public Integer maxEntries() {
      return attributes.attribute(MAX_ENTRIES).get();
   }

   public Integer factor() {
      return attributes.attribute(FACTOR).get();
   }

   public Integer minSize() {
      return attributes.attribute(MIN_SIZE).get();
   }

   public Integer maxSize() {
      return attributes.attribute(MAX_SIZE).get();
   }

   public Integer maxForcedSize() {
      return attributes.attribute(MAX_FORCED_SIZE).get();
   }

   public Boolean calibrateByDeletes() {
      return attributes.attribute(CALIBRATE_BY_DELETES).get();
   }
}
