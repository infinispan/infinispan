package org.infinispan.configuration.cache;

public class UnsafeConfigurationBuilder extends AbstractConfigurationChildBuilder<UnsafeConfiguration> {

   private boolean unreliableReturnValues = false;

   protected UnsafeConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public UnsafeConfigurationBuilder unreliableReturnValues(boolean b) {
      this.unreliableReturnValues = b;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub

   }

   @Override
   UnsafeConfiguration create() {
      return new UnsafeConfiguration(unreliableReturnValues);
   }
   
   @Override
   public UnsafeConfigurationBuilder read(UnsafeConfiguration template) {
      this.unreliableReturnValues = template.unreliableReturnValues();
      
      return this;
   }

}
