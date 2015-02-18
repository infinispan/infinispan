package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
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
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).build();
   public static final AttributeDefinition<Boolean> STORE_KEYS_AS_BINARY = AttributeDefinition.builder("storeKeysAsBinary", true).immutable().build();
   public static final AttributeDefinition<Boolean> STORE_VALUES_AS_BINARY = AttributeDefinition.builder("storeValuesAsBinary", true).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StoreAsBinaryConfiguration.class, ENABLED, STORE_KEYS_AS_BINARY, STORE_VALUES_AS_BINARY);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Boolean> storeKeysAsBinary;
   private final Attribute<Boolean> storeValuesAsBinary;
   private final AttributeSet attributes;

   StoreAsBinaryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      storeKeysAsBinary = attributes.attribute(STORE_KEYS_AS_BINARY);
      storeValuesAsBinary = attributes.attribute(STORE_VALUES_AS_BINARY);
   }

   /**
    * Enables storing both keys and values as binary.
    */
   public boolean enabled() {
      return enabled.get();
   }

   public StoreAsBinaryConfiguration enabled(boolean enabled) {
      this.enabled.set(enabled);
      return this;
   }

   /**
    * Enables storing keys as binary.
    */
   public boolean storeKeysAsBinary() {
      return storeKeysAsBinary.get();
   }

   /**
    * Enables storing values as binary.
    */
   public boolean storeValuesAsBinary() {
      return storeValuesAsBinary.get();
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

   public AttributeSet attributes() {
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
