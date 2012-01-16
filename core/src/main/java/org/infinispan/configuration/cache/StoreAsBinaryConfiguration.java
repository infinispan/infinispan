package org.infinispan.configuration.cache;

/**
 * A mechanism by which data is stored as a binary byte array. This allows
 * serialization and deserialization of objects is deferred till the point
 * in time in which they are used and needed. This typically means that any
 * deserialization happens using the thread context class loader of the
 * invocation that requires deserialization, and is an effective mechanism
 * to provide classloader isolation.
 */
public class StoreAsBinaryConfiguration {

   private final boolean enabled;
   private final boolean storeKeysAsBinary;
   private final boolean storeValuesAsBinary;
   
   StoreAsBinaryConfiguration(boolean enabled, boolean storeKeysAsBinary, boolean storeValuesAsBinary) {
      this.enabled = enabled;
      this.storeKeysAsBinary = storeKeysAsBinary;
      this.storeValuesAsBinary = storeValuesAsBinary;
   }

   public boolean enabled() {
      return enabled;
   }

   public boolean storeKeysAsBinary() {
      return storeKeysAsBinary;
   }

   public boolean storeValuesAsBinary() {
      return storeValuesAsBinary;
   }   
   
}
