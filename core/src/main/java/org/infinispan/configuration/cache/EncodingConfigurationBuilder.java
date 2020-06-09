package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.EncodingConfiguration.MEDIA_TYPE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
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
   private final Attribute<String> mediaType;

   private final AttributeSet attributes;

   EncodingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = EncodingConfiguration.attributeDefinitionSet();
      mediaType = attributes.attribute(MEDIA_TYPE);
      builders.addAll(Arrays.asList(keyContentTypeBuilder, valueContentTypeBuilder));
   }

   @Override
   public ElementDefinition<?> getElementDefinition() {
      return EncodingConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public void validate() {
      String globalMediaType = mediaType.get();
      if (globalMediaType != null) {
         if ((keyContentTypeBuilder.mediaType() != null || valueContentTypeBuilder.mediaType() != null)) {
            CONFIG.ignoringSpecificMediaTypes();
         }
         keyContentTypeBuilder.mediaType(globalMediaType);
         valueContentTypeBuilder.mediaType(globalMediaType);
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

   public EncodingConfigurationBuilder mediaType(String keyValueMediaType) {
      mediaType.set(keyValueMediaType);
      return this;
   }

   public boolean isStorageBinary() {
      String keyMediaType = keyContentTypeBuilder.mediaType();
      String valueMediaType = valueContentTypeBuilder.mediaType();
      return keyMediaType != null && valueMediaType != null &&
            MediaType.fromString(keyMediaType).isBinary() && MediaType.fromString(valueMediaType).isBinary();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public EncodingConfiguration create() {
      ContentTypeConfiguration keyContentType = keyContentTypeBuilder.create();
      ContentTypeConfiguration valueContentType = valueContentTypeBuilder.create();
      return new EncodingConfiguration(attributes.protect(), keyContentType, valueContentType);
   }

   @Override
   public Builder<?> read(EncodingConfiguration template) {
      this.attributes.read(template.attributes());
      this.keyContentTypeBuilder = new ContentTypeConfigurationBuilder(true, this).read(template.keyDataType());
      this.valueContentTypeBuilder = new ContentTypeConfigurationBuilder(false, this).read(template.valueDataType());
      return this;
   }


   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public String toString() {
      return "EncodingConfigurationBuilder{" +
            "keyContentTypeBuilder=" + keyContentTypeBuilder +
            ", valueContentTypeBuilder=" + valueContentTypeBuilder +
            ", attributes=" + attributes +
            '}';
   }


   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return builders;
   }
}
