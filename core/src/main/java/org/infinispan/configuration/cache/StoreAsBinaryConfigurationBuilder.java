package org.infinispan.configuration.cache;


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
      enabled = true;
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
   
}
