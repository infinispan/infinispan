package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.ENCODING;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Controls encoding configuration for keys and values in the cache.
 *
 * @since 9.2
 */
public final class EncodingConfiguration implements Matchable<EncodingConfiguration>, ConfigurationInfo {

   private ContentTypeConfiguration keyDataType, valueDataType;
   private final List<ConfigurationInfo> contentTypeConfigurations;

   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(ENCODING.getLocalName());

   public EncodingConfiguration(ContentTypeConfiguration keyDataType, ContentTypeConfiguration valueDataType) {
      this.keyDataType = keyDataType;
      this.valueDataType = valueDataType;
      this.contentTypeConfigurations = Arrays.asList(keyDataType, valueDataType);
   }

   public ContentTypeConfiguration keyDataType() {
      return keyDataType;
   }

   public ContentTypeConfiguration valueDataType() {
      return valueDataType;
   }

   @Override
   public String toString() {
      return "DataTypeConfiguration [keyDataType=" + keyDataType + ", valueDataType=" + valueDataType + "]";
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EncodingConfiguration that = (EncodingConfiguration) o;
      return Objects.equals(keyDataType, that.keyDataType) &&
            Objects.equals(valueDataType, that.valueDataType);
   }

   @Override
   public int hashCode() {
      return Objects.hash(keyDataType, valueDataType);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return contentTypeConfigurations;
   }
}
