package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ContentTypeConfiguration.MEDIA_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class ContentTypeConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ContentTypeConfiguration> {

   private final AttributeSet attributes;

   protected ContentTypeConfigurationBuilder(EncodingConfigurationBuilder builder) {
      super(builder.getBuilder());
      attributes = ContentTypeConfiguration.attributeDefinitionSet();
   }

   @Override
   public void validate() {
   }

   public ContentTypeConfigurationBuilder mediaType(String mediaType) {
      attributes.attribute(MEDIA_TYPE).set(mediaType);
      return this;
   }

   @Override
   public ContentTypeConfiguration create() {
      return new ContentTypeConfiguration(attributes.protect());
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
