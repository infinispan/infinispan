package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ContentTypeConfiguration.MEDIA_TYPE;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class ContentTypeConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ContentTypeConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;
   private final boolean key;
   private MediaType parsed;

   protected ContentTypeConfigurationBuilder(boolean key, EncodingConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.key = key;
      attributes = ContentTypeConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition<?> getElementDefinition() {
      return key ? ContentTypeConfiguration.KEY_ELEMENT_DEFINITION : ContentTypeConfiguration.VALUE_ELEMENT_DEFINITION;
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
      return new ContentTypeConfiguration(key, attributes.protect(), parsed);
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
