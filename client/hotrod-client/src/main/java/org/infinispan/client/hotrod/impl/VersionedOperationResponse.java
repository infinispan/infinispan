package org.infinispan.client.hotrod.impl;

/**
* // TODO: Document this
*
* @author Mircea.Markus@jboss.com
* @since 4.1
*/
public class VersionedOperationResponse {

   public enum RspCode {
      SUCCESS(true), NO_SUCH_KEY(false), MODIFIED_KEY(false);
      private boolean isModified;

      RspCode(boolean modified) {
         isModified = modified;
      }

      public boolean isUpdated() {
         return isModified;
      }
   }

   private byte[] value;

   private RspCode code;


   public VersionedOperationResponse(byte[] value, RspCode code) {
      this.value = value;
      this.code = code;
   }

   public byte[] getValue() {
      return value;
   }

   public RspCode getCode() {
      return code;
   }
}
