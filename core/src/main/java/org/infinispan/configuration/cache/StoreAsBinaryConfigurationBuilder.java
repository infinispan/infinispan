package org.infinispan.configuration.cache;

/**
 * A mechanism by which data is stored as a binary byte array. This allows
 * serialization and deserialization of objects is deferred till the point
 * in time in which they are used and needed. This typically means that any
 * deserialization happens using the thread context class loader of the
 * invocation that requires deserialization, and is an effective mechanism
 * to provide classloader isolation.
 */
public class StoreAsBinaryConfigurationBuilder extends AbstractConfigurationChildBuilder<StoreAsBinaryConfiguration> {

   private boolean enabled = false;
   private boolean storeKeysAsBinary = true;
   private boolean storeValuesAsBinary = true;
   
   StoreAsBinaryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public StoreAsBinaryConfigurationBuilder enable() {
      enabled = true;
      return this;
   }
   
   public StoreAsBinaryConfigurationBuilder disable() {
      enabled = false;
      return this;
   }
   
   public StoreAsBinaryConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public StoreAsBinaryConfigurationBuilder storeKeysAsBinary(boolean b) {
      this.storeKeysAsBinary = b;
      return this;
   }

   public StoreAsBinaryConfigurationBuilder storeValuesAsBinary(boolean b) {
      this.storeValuesAsBinary = b;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   StoreAsBinaryConfiguration create() {
      return new StoreAsBinaryConfiguration(enabled, storeKeysAsBinary, storeValuesAsBinary);
   }   
   
   public StoreAsBinaryConfigurationBuilder read(StoreAsBinaryConfiguration template) {
      this.enabled = template.enabled();
      this.storeKeysAsBinary = template.storeKeysAsBinary();
      this.storeValuesAsBinary = template.storeValuesAsBinary();
      
      return this;
   }
}
