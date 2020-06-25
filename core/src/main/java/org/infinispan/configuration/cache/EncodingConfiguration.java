package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.ENCODING;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Controls encoding configuration for keys and values in the cache.
 *
 * @since 9.2
 */
public final class EncodingConfiguration implements Matchable<EncodingConfiguration>, ConfigurationInfo {

   static final AttributeDefinition<String> MEDIA_TYPE = AttributeDefinition.builder("mediaType", null, String.class).build();
   private final AttributeSet attributes;

   private final ContentTypeConfiguration keyDataType, valueDataType;
   private final List<ConfigurationInfo> contentTypeConfigurations;

   static final ElementDefinition<EncodingConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(ENCODING.getLocalName());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EncodingConfiguration.class, MEDIA_TYPE);
   }

   public EncodingConfiguration(AttributeSet attributes, ContentTypeConfiguration keyDataType, ContentTypeConfiguration valueDataType) {
      this.attributes = attributes;
      this.keyDataType = keyDataType;
      this.valueDataType = valueDataType;
      this.contentTypeConfigurations = Arrays.asList(keyDataType, valueDataType);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public ContentTypeConfiguration keyDataType() {
      return keyDataType;
   }

   public ContentTypeConfiguration valueDataType() {
      return valueDataType;
   }

   @Override
   public String toString() {
      return "EncodingConfiguration{" +
            "attributes=" + attributes +
            ", keyDataType=" + keyDataType +
            ", valueDataType=" + valueDataType +
            '}';
   }

   @Override
   public ElementDefinition<EncodingConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EncodingConfiguration that = (EncodingConfiguration) o;

      if (!attributes.equals(that.attributes)) return false;
      if (!keyDataType.equals(that.keyDataType)) return false;
      return valueDataType.equals(that.valueDataType);
   }

   @Override
   public int hashCode() {
      int result = attributes.hashCode();
      result = 31 * result + keyDataType.hashCode();
      result = 31 * result + valueDataType.hashCode();
      return result;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return contentTypeConfigurations;
   }
}
