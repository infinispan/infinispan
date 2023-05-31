package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.parsing.Attribute;

/**
 * @since 9.2
 */
public class ContentTypeConfiguration extends ConfigurationElement<ContentTypeConfiguration> {
   public static final AttributeDefinition<MediaType> MEDIA_TYPE =
         AttributeDefinition.builder(Attribute.MEDIA_TYPE, null, MediaType.class).immutable().build();

   private final MediaType mediaType;

   ContentTypeConfiguration(Enum<?> element, AttributeSet attributes, MediaType parentType) {
      super(element, attributes);
      // parent type has precedence
      this.mediaType = parentType != null ? parentType : attributes.attribute(MEDIA_TYPE).get();
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ContentTypeConfiguration.class, MEDIA_TYPE);
   }

   public MediaType mediaType() {
      return mediaType;
   }

   public boolean isMediaTypeChanged() {
      return mediaType != null;
   }

   public boolean isObjectStorage() {
      return MediaType.APPLICATION_OBJECT.match(mediaType);
   }

   public boolean isProtoBufStorage() {
      return MediaType.APPLICATION_PROTOSTREAM.match(mediaType);
   }
}
