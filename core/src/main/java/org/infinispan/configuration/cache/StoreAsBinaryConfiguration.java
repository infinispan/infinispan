package org.infinispan.configuration.cache;

/**
 * Controls whether when stored in memory, keys and values are stored as references to their original objects, or in
 * a serialized, binary format.  There are benefits to both approaches, but often if used in a clustered mode,
 * storing objects as binary means that the cost of serialization happens early on, and can be amortized.  Further,
 * deserialization costs are incurred lazily which improves throughput.
 * <p />
 * It is possible to control this on a fine-grained basis: you can choose to just store keys or values as binary, or
 * both.
 * <p />
 * @see StoreAsBinaryConfigurationBuilder
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

   /**
    * Enables storing both keys and values as binary.
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * Enables storing keys as binary.
    */
   public boolean storeKeysAsBinary() {
      return storeKeysAsBinary;
   }

   /**
    * Enables storing values as binary.
    */
   public boolean storeValuesAsBinary() {
      return storeValuesAsBinary;
   }   
   
}
