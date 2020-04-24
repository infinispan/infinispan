package org.infinispan.configuration.cache;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 9.2
 */
public class EncodingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<EncodingConfiguration>, ConfigurationBuilderInfo {

   private ContentTypeConfigurationBuilder keyContentTypeBuilder = new ContentTypeConfigurationBuilder(true, this);
   private ContentTypeConfigurationBuilder valueContentTypeBuilder = new ContentTypeConfigurationBuilder(false, this);
   private List<ConfigurationBuilderInfo> builders = new ArrayList<>();
   private String mediaType;

   EncodingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      builders.addAll(Arrays.asList(keyContentTypeBuilder, valueContentTypeBuilder));
   }

   @Override
   public ElementDefinition<?> getElementDefinition() {
      return EncodingConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public void validate() {
      if (mediaType != null) {
         if ((keyContentTypeBuilder.mediaType() != null || valueContentTypeBuilder.mediaType() != null)) {
            CONFIG.ignoringSpecificMediaTypes();
         }
      }
      keyContentTypeBuilder.validate();
      valueContentTypeBuilder.validate();
   }

   public boolean isObjectStorage() {
      return keyContentTypeBuilder.isObjectStorage() && valueContentTypeBuilder.isObjectStorage();
   }

   public ContentTypeConfigurationBuilder key() {
      return keyContentTypeBuilder;
   }

   public ContentTypeConfigurationBuilder value() {
      return valueContentTypeBuilder;
   }

   public EncodingConfigurationBuilder mediaType(String mediaType) {
      this.mediaType = mediaType;
      keyContentTypeBuilder.mediaType(mediaType);
      valueContentTypeBuilder.mediaType(mediaType);
      return this;
   }

   public boolean isStorageBinary() {
      String keyMediaType = keyContentTypeBuilder.mediaType();
      String valueMediaType = valueContentTypeBuilder.mediaType();
      return keyMediaType != null && valueMediaType != null &&
            MediaType.fromString(keyMediaType).isBinary() && MediaType.fromString(valueMediaType).isBinary();
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
