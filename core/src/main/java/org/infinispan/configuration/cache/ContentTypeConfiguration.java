package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.KEY_DATA_TYPE;
import static org.infinispan.configuration.parsing.Element.VALUE_DATA_TYPE;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 9.2
 */
public class ContentTypeConfiguration implements ConfigurationInfo {

   public static final String DEFAULT_MEDIA_TYPE = MediaType.APPLICATION_OBJECT_TYPE;

   public static final ElementDefinition KEY_ELEMENT_DEFINITION = new DefaultElementDefinition(KEY_DATA_TYPE.getLocalName());

   public static final ElementDefinition VALUE_ELEMENT_DEFINITION = new DefaultElementDefinition(VALUE_DATA_TYPE.getLocalName());

   public static final AttributeDefinition<String> MEDIA_TYPE =
         AttributeDefinition.builder("media-type", null, String.class).build();

   private final Attribute<String> mediaType;
   private final boolean key;
   private final AttributeSet attributes;

   ContentTypeConfiguration(boolean key, AttributeSet attributes) {
      this.key = key;
      this.attributes = attributes.checkProtection();
      mediaType = attributes.attribute(MEDIA_TYPE);
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ContentTypeConfiguration.class, MEDIA_TYPE);
   }

   public MediaType mediaType() {
      if(mediaType.isNull()) return null;
      return MediaType.fromString(mediaType.get());
   }

   public void mediaType(MediaType mediaType) {
      attributes.attribute(MEDIA_TYPE).set(mediaType.toString());
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return key ? KEY_ELEMENT_DEFINITION : VALUE_ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return Collections.emptyList();
   }

   public boolean isMediaTypeChanged() {
      return attributes.attribute(MEDIA_TYPE).isModified();
   }

   public boolean isEncodingChanged() {
      return attributes.attribute(MEDIA_TYPE).isModified();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ContentTypeConfiguration that = (ContentTypeConfiguration) o;

      if (key != that.key) return false;
      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      int result = (key ? 1 : 0);
      result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "ContentTypeConfiguration [attributes=" + attributes + "]";
   }
}
