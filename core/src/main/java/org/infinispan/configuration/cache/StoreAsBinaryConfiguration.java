package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

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
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).build();
   static final AttributeDefinition<Boolean> STORE_KEYS_AS_BINARY = AttributeDefinition.builder("storeKeysAsBinary", true).immutable().build();
   static final AttributeDefinition<Boolean> STORE_VALUES_AS_BINARY = AttributeDefinition.builder("storeValuesAsBinary", true).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(StoreAsBinaryConfiguration.class, ENABLED, STORE_KEYS_AS_BINARY, STORE_VALUES_AS_BINARY);
   }

   private final AttributeSet attributes;

   StoreAsBinaryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();;
   }

   /**
    * Enables storing both keys and values as binary.
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   public StoreAsBinaryConfiguration enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Enables storing keys as binary.
    */
   public boolean storeKeysAsBinary() {
      return attributes.attribute(STORE_KEYS_AS_BINARY).asBoolean();
   }

   /**
    * Enables storing values as binary.
    */
   public boolean storeValuesAsBinary() {
      return attributes.attribute(STORE_VALUES_AS_BINARY).asBoolean();
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

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "StoreAsBinaryConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      StoreAsBinaryConfiguration other = (StoreAsBinaryConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
