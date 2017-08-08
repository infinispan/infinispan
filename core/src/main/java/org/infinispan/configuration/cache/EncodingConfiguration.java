package org.infinispan.configuration.cache;

import java.util.Objects;

/**
 * Controls encoding configuration for keys and values in the cache.
 *
 * @since 9.2
 */
public final class EncodingConfiguration {

   private ContentTypeConfiguration keyDataType, valueDataType;

   public EncodingConfiguration(ContentTypeConfiguration keyDataType, ContentTypeConfiguration valueDataType) {
      this.keyDataType = keyDataType;
      this.valueDataType = valueDataType;
   }

   public ContentTypeConfiguration keyDataType() {
      return keyDataType;
   }

   public ContentTypeConfiguration valueDataType() {
      return valueDataType;
   }

   @Override
   public String toString() {
      return "DataTypeConfiguration [keyDataType=" + keyDataType + ", valueDataType=" + valueDataType + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EncodingConfiguration that = (EncodingConfiguration) o;
      return Objects.equals(keyDataType, that.keyDataType) &&
            Objects.equals(valueDataType, that.valueDataType);
   }

   @Override
   public int hashCode() {
      return Objects.hash(keyDataType, valueDataType);
   }

}
