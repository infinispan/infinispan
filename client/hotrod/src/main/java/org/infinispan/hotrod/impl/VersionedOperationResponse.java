package org.infinispan.hotrod.impl;

/**
* @author Mircea.Markus@jboss.com
* @since 14.0
*/
public class VersionedOperationResponse<V> {

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

   private V value;

   private RspCode code;


   public VersionedOperationResponse(V value, RspCode code) {
      this.value = value;
      this.code = code;
   }

   public V getValue() {
      return value;
   }

   public RspCode getCode() {
      return code;
   }
}
