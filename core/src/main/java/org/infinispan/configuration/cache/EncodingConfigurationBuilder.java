package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class EncodingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<EncodingConfiguration>, ConfigurationBuilderInfo {

   private ContentTypeConfigurationBuilder keyContentTypeBuilder = new ContentTypeConfigurationBuilder(true, this);
   private ContentTypeConfigurationBuilder valueContentTypeBuilder = new ContentTypeConfigurationBuilder(false, this);
   private List<ConfigurationBuilderInfo> builders = new ArrayList<>();


   EncodingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      builders.addAll(Arrays.asList(keyContentTypeBuilder, valueContentTypeBuilder));
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return EncodingConfiguration.ELEMENT_DEFINITION;
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
      this.keyContentTypeBuilder = new ContentTypeConfigurationBuilder(true, this).read(template.keyDataType());
      this.valueContentTypeBuilder = new ContentTypeConfigurationBuilder(false, this).read(template.valueDataType());
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


   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return builders;
   }
}
