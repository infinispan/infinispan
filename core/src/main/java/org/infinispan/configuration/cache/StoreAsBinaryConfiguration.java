package org.infinispan.configuration.cache;

public class StoreAsBinaryConfiguration {

   private final boolean enabled;
   private final boolean storeKeysAsBinary;
   private final boolean storeValuesAsBinary;
   
   StoreAsBinaryConfiguration(boolean enabled, boolean storeKeysAsBinary, boolean storeValuesAsBinary) {
      this.enabled = enabled;
      this.storeKeysAsBinary = storeKeysAsBinary;
      this.storeValuesAsBinary = storeValuesAsBinary;
   }

   public boolean isEnabled() {
      return enabled;
   }

   public boolean isStoreKeysAsBinary() {
      return storeKeysAsBinary;
   }

   public boolean isStoreValuesAsBinary() {
      return storeValuesAsBinary;
   }   
   
}
