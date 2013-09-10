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

   private boolean enabled;
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

   public StoreAsBinaryConfiguration enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
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

   /**
    * Enables defensive copies.
    *
    * @deprecated Store as binary configuration is always defensive now.
    */
   @Deprecated
   public boolean defensive() {
      return true;
   }

   @Override
   public String toString() {
      return "StoreAsBinaryConfiguration{" +
            "enabled=" + enabled +
            ", storeKeysAsBinary=" + storeKeysAsBinary +
            ", storeValuesAsBinary=" + storeValuesAsBinary +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StoreAsBinaryConfiguration that = (StoreAsBinaryConfiguration) o;

      if (enabled != that.enabled) return false;
      if (storeKeysAsBinary != that.storeKeysAsBinary) return false;
      if (storeValuesAsBinary != that.storeValuesAsBinary) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (storeKeysAsBinary ? 1 : 0);
      result = 31 * result + (storeValuesAsBinary ? 1 : 0);
      return result;
   }

}
