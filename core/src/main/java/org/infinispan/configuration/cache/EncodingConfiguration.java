package org.infinispan.configuration.cache;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

/**
 * Controls encoding configuration for keys and values in the cache.
 *
 * @since 9.2
 */
public final class EncodingConfiguration extends ConfigurationElement<EncodingConfiguration> {
   static final AttributeDefinition<MediaType> MEDIA_TYPE = AttributeDefinition.builder(Attribute.MEDIA_TYPE, null, MediaType.class).immutable().build();
   private final ContentTypeConfiguration keyDataType, valueDataType;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EncodingConfiguration.class, MEDIA_TYPE);
   }

   public EncodingConfiguration(AttributeSet attributes, ContentTypeConfiguration keyDataType, ContentTypeConfiguration valueDataType) {
      super(Element.ENCODING, attributes, keyDataType, valueDataType);
      this.keyDataType = keyDataType;
      this.valueDataType = valueDataType;
   }

   public ContentTypeConfiguration keyDataType() {
      return keyDataType;
   }

   public ContentTypeConfiguration valueDataType() {
      return valueDataType;
   }

   @Override
   public boolean matches(EncodingConfiguration other) {
      return Objects.equals(this.keyDataType.mediaType(), other.keyDataType.mediaType()) &&
            Objects.equals(this.valueDataType.mediaType(), other.valueDataType.mediaType());
   }
}
