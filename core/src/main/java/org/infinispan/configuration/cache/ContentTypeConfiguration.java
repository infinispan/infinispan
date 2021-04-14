package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 9.2
 */
public class ContentTypeConfiguration extends ConfigurationElement<ContentTypeConfiguration> {

   public static final String DEFAULT_MEDIA_TYPE = MediaType.APPLICATION_OBJECT_TYPE;

   public static final AttributeDefinition<String> MEDIA_TYPE =
         AttributeDefinition.builder("media-type", null, String.class).build();

   private final MediaType parsed;

   ContentTypeConfiguration(Enum<?> element, AttributeSet attributes, MediaType parsed) {
      super(element, attributes);
      this.parsed = parsed;
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ContentTypeConfiguration.class, MEDIA_TYPE);
   }

   public MediaType mediaType() {
      return parsed;
   }

   public void mediaType(MediaType mediaType) {
      attributes.attribute(MEDIA_TYPE).set(mediaType.toString());
   }

   public boolean isMediaTypeChanged() {
      return attributes.attribute(MEDIA_TYPE).isModified();
   }

   public boolean isEncodingChanged() {
      return attributes.attribute(MEDIA_TYPE).isModified();
   }

   public boolean isObjectStorage() {
      String mediaType = attributes.attribute(MEDIA_TYPE).get();
      return mediaType != null && MediaType.fromString(mediaType).match(MediaType.APPLICATION_OBJECT);
   }

   public boolean isProtobufStorage() {
      String mediaType = attributes.attribute(MEDIA_TYPE).get();
      return mediaType != null && MediaType.fromString(mediaType).match(MediaType.APPLICATION_PROTOSTREAM);
   }
}
