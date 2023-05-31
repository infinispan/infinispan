package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ContentTypeConfiguration.MEDIA_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class ContentTypeConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ContentTypeConfiguration> {

   private final AttributeSet attributes;
   private final Enum<?> element;

   protected ContentTypeConfigurationBuilder(Enum<?> element, EncodingConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.element = element;
      attributes = ContentTypeConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public boolean isObjectStorage() {
      return MediaType.APPLICATION_OBJECT.match(mediaType());
   }

   public boolean isProtobufStorage() {
      return MediaType.APPLICATION_PROTOSTREAM.match(mediaType());
   }

   public ContentTypeConfigurationBuilder mediaType(String mediaType) {
      attributes.attribute(MEDIA_TYPE).set(MediaType.fromString(mediaType));
      return this;
   }

   public ContentTypeConfigurationBuilder mediaType(MediaType mediaType) {
      attributes.attribute(MEDIA_TYPE).set(mediaType);
      return this;
   }

   public MediaType mediaType() {
      return attributes.attribute(MEDIA_TYPE).get();
   }

   @Override
   public ContentTypeConfiguration create() {
      throw new UnsupportedOperationException();
   }

   ContentTypeConfiguration create(MediaType globalType) {
      return new ContentTypeConfiguration(element, attributes.protect(), globalType);
   }

   @Override
   public ContentTypeConfigurationBuilder read(ContentTypeConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }
}
