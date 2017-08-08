package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class EncodingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<EncodingConfiguration> {

   private ContentTypeConfigurationBuilder keyContentTypeBuilder = new ContentTypeConfigurationBuilder(this);
   private ContentTypeConfigurationBuilder valueContentTypeBuilder = new ContentTypeConfigurationBuilder(this);

   EncodingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public void validate() {
      keyContentTypeBuilder.validate();
      valueContentTypeBuilder.validate();
   }

   public ContentTypeConfigurationBuilder key() {
      return keyContentTypeBuilder;
   }

   public ContentTypeConfigurationBuilder value() {
      return valueContentTypeBuilder;
   }

   @Override
   public EncodingConfiguration create() {
      ContentTypeConfiguration keyContentType = keyContentTypeBuilder.create();
      ContentTypeConfiguration valueContentType = valueContentTypeBuilder.create();
      return new EncodingConfiguration(keyContentType, valueContentType);
   }

   @Override
   public Builder<?> read(EncodingConfiguration template) {
      this.keyContentTypeBuilder = new ContentTypeConfigurationBuilder(this).read(template.keyDataType());
      this.valueContentTypeBuilder = new ContentTypeConfigurationBuilder(this).read(template.valueDataType());
      return this;
   }


   @Override
   public void validate(GlobalConfiguration globalConfig) {
      keyContentTypeBuilder.validate();
      valueContentTypeBuilder.validate();
   }

   @Override
   public String toString() {
      return "DataTypeConfigurationBuilder{" +
            "keyContentTypeBuilder=" + keyContentTypeBuilder +
            ", valueContentTypeBuilder=" + valueContentTypeBuilder +
            '}';
   }
}
