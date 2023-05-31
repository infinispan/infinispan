package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.EncodingConfiguration.MEDIA_TYPE;
import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 9.2
 */
public class EncodingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<EncodingConfiguration> {

   private ContentTypeConfigurationBuilder keyContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.KEY_DATA_TYPE, this);
   private ContentTypeConfigurationBuilder valueContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.VALUE_DATA_TYPE, this);
   private final Attribute<MediaType> mediaType;

   private final AttributeSet attributes;

   EncodingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = EncodingConfiguration.attributeDefinitionSet();
      mediaType = attributes.attribute(MEDIA_TYPE);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public void validate() {
      MediaType globalMediaType = mediaType.get();
      if (globalMediaType != null) {
         MediaType keyType = keyContentTypeBuilder.mediaType();
         MediaType valueType = valueContentTypeBuilder.mediaType();
         if ((keyType != null && !keyType.equals(globalMediaType)) || valueType != null && !valueType.equals(globalMediaType)) {
            CONFIG.ignoringSpecificMediaTypes();
         }
      }
      keyContentTypeBuilder.validate();
      valueContentTypeBuilder.validate();
   }

   public boolean isObjectStorage() {
      if (!mediaType.isNull()) {
         return MediaType.APPLICATION_OBJECT.match(mediaType.get());
      } else {
         return keyContentTypeBuilder.isObjectStorage() && valueContentTypeBuilder.isObjectStorage();
      }
   }

   public ContentTypeConfigurationBuilder key() {
      return keyContentTypeBuilder;
   }

   public ContentTypeConfigurationBuilder value() {
      return valueContentTypeBuilder;
   }

   public EncodingConfigurationBuilder mediaType(String keyValueMediaType) {
      mediaType.set(MediaType.fromString(keyValueMediaType));
      return this;
   }

   public EncodingConfigurationBuilder mediaType(MediaType keyValueMediaType) {
      mediaType.set(keyValueMediaType);
      return this;
   }

   public boolean isStorageBinary() {
      // global takes precedence
      if (!mediaType.isNull()) {
         return mediaType.get().isBinary();
      } else {
         MediaType keyMediaType = keyContentTypeBuilder.mediaType();
         MediaType valueMediaType = valueContentTypeBuilder.mediaType();
         return keyMediaType != null && valueMediaType != null &&
               keyMediaType.isBinary() && valueMediaType.isBinary();
      }
   }

   @Override
   public EncodingConfiguration create() {
      MediaType globalType = mediaType.get();
      ContentTypeConfiguration keyContentType = keyContentTypeBuilder.create(globalType);
      ContentTypeConfiguration valueContentType = valueContentTypeBuilder.create(globalType);
      return new EncodingConfiguration(attributes.protect(), keyContentType, valueContentType);
   }

   @Override
   public Builder<?> read(EncodingConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.keyContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.KEY_DATA_TYPE, this).read(template.keyDataType(), combine);
      this.valueContentTypeBuilder = new ContentTypeConfigurationBuilder(Element.VALUE_DATA_TYPE, this).read(template.valueDataType(), combine);
      return this;
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
