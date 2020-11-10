package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.INDEX_READER;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 12.0
 */
public class IndexReaderConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Long> REFRESH_INTERVAL =
         AttributeDefinition.builder("refresh-interval", 0L, Long.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexReaderConfiguration.class, REFRESH_INTERVAL);
   }

   static final ElementDefinition<IndexReaderConfiguration> ELEMENT_DEFINITION =
         new DefaultElementDefinition<>(INDEX_READER.getLocalName());

   private final AttributeSet attributes;
   private final Attribute<Long> refreshInternal;

   IndexReaderConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.refreshInternal = attributes.attribute(REFRESH_INTERVAL);
   }

   @Override
   public ElementDefinition<IndexReaderConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public long getRefreshInterval() {
      return refreshInternal.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IndexReaderConfiguration that = (IndexReaderConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "IndexReaderConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
