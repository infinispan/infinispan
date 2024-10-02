package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.MetadataValue;

/**
* @author Mircea.Markus@jboss.com
* @since 4.1
*/
public class VersionedOperationResponse<V> {

   public enum RspCode {
      SUCCESS(true), NO_SUCH_KEY(false), MODIFIED_KEY(false);
      private final boolean isModified;

      RspCode(boolean modified) {
         isModified = modified;
      }

      public boolean isUpdated() {
         return isModified;
      }
   }

   private final MetadataValue<V> metadata;

   private final RspCode code;


   public VersionedOperationResponse(MetadataValue<V> metadata, RspCode code) {
      this.metadata = metadata;
      this.code = code;
   }

   public V getValue() {
      return metadata == null ? null : metadata.getValue();
   }

   public MetadataValue<V> getMetadata() {
      return metadata;
   }

   public RspCode getCode() {
      return code;
   }
}
