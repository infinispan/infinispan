package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ContentTypeConfiguration.MEDIA_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class ContentTypeConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ContentTypeConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;
   private final boolean key;

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
   public ElementDefinition getElementDefinition() {
      return key ? ContentTypeConfiguration.KEY_ELEMENT_DEFINITION : ContentTypeConfiguration.VALUE_ELEMENT_DEFINITION;
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
      return new ContentTypeConfiguration(key, attributes.protect());
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
