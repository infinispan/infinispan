package org.infinispan.client.hotrod;

import org.infinispan.commons.api.CacheContainerAdmin;

/**
 * Flags which affect only administrative operations
 * @author Tristan Tarrant
 * @since 9.0
 * @deprecated use {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag} instead
 */
@Deprecated
public enum AdminFlag {
   /**
    * If the operation affects configuration, make it persistent. If the server cannot honor this flag an error will
    * be returned
    */
   PERSISTENT("persistent", CacheContainerAdmin.AdminFlag.PERSISTENT);


   private final String value;
   private final CacheContainerAdmin.AdminFlag newFlag;

   AdminFlag(String value, CacheContainerAdmin.AdminFlag newFlag) {
      this.value = value;
      this.newFlag = newFlag;
   }

   @Override
   public String toString() {
      return value;
   }

   public CacheContainerAdmin.AdminFlag upgrade() {
      return newFlag;
   }
}
