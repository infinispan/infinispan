package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.EncodingConfiguration.MEDIA_TYPE;
import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 9.2
 */
public class EncodingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<EncodingConfiguration> {

   private ContentTypeConfigurationBuilder keyContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.KEY_DATA_TYPE, this);
   private ContentTypeConfigurationBuilder valueContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.VALUE_DATA_TYPE, this);
   private final Attribute<String> mediaType;

   private final AttributeSet attributes;

   EncodingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = EncodingConfiguration.attributeDefinitionSet();
      mediaType = attributes.attribute(MEDIA_TYPE);
   }

   @Override
   public void validate() {
      String globalMediaType = mediaType.get();
      if (globalMediaType != null) {
         String keyType = keyContentTypeBuilder.mediaType();
         String valueType = valueContentTypeBuilder.mediaType();
         if ((keyType != null && !keyType.equals(globalMediaType)) || valueType != null && !valueType.equals(globalMediaType)) {
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
   public EncodingConfiguration create() {
      ContentTypeConfiguration keyContentType = keyContentTypeBuilder.create();
      ContentTypeConfiguration valueContentType = valueContentTypeBuilder.create();
      return new EncodingConfiguration(attributes.protect(), keyContentType, valueContentType);
   }

   @Override
   public Builder<?> read(EncodingConfiguration template) {
      this.attributes.read(template.attributes());
      this.keyContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.KEY_DATA_TYPE, this).read(template.keyDataType());
      this.valueContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.VALUE_DATA_TYPE, this).read(template.valueDataType());
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
}
