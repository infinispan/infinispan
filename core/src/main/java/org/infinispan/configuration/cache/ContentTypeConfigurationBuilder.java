package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ContentTypeConfiguration.MEDIA_TYPE;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class ContentTypeConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ContentTypeConfiguration> {

   private final AttributeSet attributes;
   private final Enum<?> element;
   private MediaType parsed;

   protected ContentTypeConfigurationBuilder(Enum<?> element, EncodingConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.element = element;
      attributes = ContentTypeConfiguration.attributeDefinitionSet();
   }

   public boolean isObjectStorage() {
      String mediaType = mediaType();
      return mediaType != null && MediaType.fromString(mediaType).match(MediaType.APPLICATION_OBJECT);
   }

   public boolean isProtobufStorage() {
      String mediaType = mediaType();
      return mediaType != null && MediaType.fromString(mediaType).match(MediaType.APPLICATION_PROTOSTREAM);
   }

   @Override
   public void validate() {
   }

   public ContentTypeConfigurationBuilder mediaType(String mediaType) {
      attributes.attribute(MEDIA_TYPE).set(mediaType);
      return this;
   }

   public String mediaType() {
      return attributes.attribute(MEDIA_TYPE).get();
   }

   @Override
   public ContentTypeConfiguration create() {
      try {
         String mediaType = attributes.attribute(MEDIA_TYPE).get();
         if (mediaType != null) this.parsed = MediaType.fromString(mediaType);
      } catch (EncodingException e) {
         throw new CacheConfigurationException(e);
      }
      return new ContentTypeConfiguration(element, attributes.protect(), parsed);
   }

   @Override
   public ContentTypeConfigurationBuilder read(ContentTypeConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }
}
