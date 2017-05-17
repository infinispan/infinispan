package org.infinispan.client.hotrod;

/**
 * Flags which affect only administrative operations
 * @author Tristan Tarrant
 * @since 9.0
 */
public enum AdminFlag {
   /**
    * If the operation affects configuration, make it persistent. If the server cannot honor this flag an error will
    * be returned
    */
   PERSISTENT("persistent");


   private final String value;

   AdminFlag(String value) {
      this.value = value;
   }


   @Override
   public String toString() {
      return value;
   }
}
