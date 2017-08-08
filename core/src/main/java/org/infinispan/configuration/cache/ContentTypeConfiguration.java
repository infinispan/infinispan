package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 9.2
 */
public class ContentTypeConfiguration {

   public static final String DEFAULT_MEDIA_TYPE = MediaType.APPLICATION_OBJECT_TYPE;

   public static final AttributeDefinition<String> MEDIA_TYPE =
         AttributeDefinition.builder("media-type", null, String.class).build();

   private final Attribute<String> mediaType;
   private final AttributeSet attributes;

   ContentTypeConfiguration(AttributeSet attributes) {
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

   public boolean isMediaTypeChanged() {
      return attributes.attribute(MEDIA_TYPE).isModified();
   }

   public boolean isEncodingChanged() {
      return attributes.attribute(MEDIA_TYPE).isModified();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ContentTypeConfiguration other = (ContentTypeConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public String toString() {
      return "ContentTypeConfiguration [attributes=" + attributes + "]";
   }
}
