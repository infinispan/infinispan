package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.INDEX_MERGE;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 12.0
 */
public class IndexMergeConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Integer> MAX_DOCS =
         AttributeDefinition.builder("max-docs", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> FACTOR =
         AttributeDefinition.builder("factor", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> MIN_SIZE =
         AttributeDefinition.builder("min-size", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> MAX_SIZE =
         AttributeDefinition.builder("max-size", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> MAX_FORCED_SIZE =
         AttributeDefinition.builder("max-forced-size", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Boolean> CALIBRATE_BY_DELETES =
         AttributeDefinition.builder("calibrate-by-deletes", null, Boolean.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexMergeConfiguration.class, MAX_DOCS, FACTOR, MIN_SIZE, MAX_SIZE, MAX_FORCED_SIZE,
            CALIBRATE_BY_DELETES);
   }

   static final ElementDefinition<IndexMergeConfiguration> ELEMENT_DEFINITION =
         new DefaultElementDefinition<>(INDEX_MERGE.getLocalName());

   private final AttributeSet attributes;

   IndexMergeConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition<IndexMergeConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public Integer maxDocs() {
      return attributes.attribute(MAX_DOCS).get();
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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IndexMergeConfiguration that = (IndexMergeConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "IndexMergeConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
