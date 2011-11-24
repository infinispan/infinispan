package org.infinispan.configuration.cache;


public class LazyDeserializationConfigurationBuilder extends AbstractConfigurationChildBuilder<LazyDeserializationConfiguration> {

   private boolean enabled = false;
   
   LazyDeserializationConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   public LazyDeserializationConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public LazyDeserializationConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   public LazyDeserializationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   LazyDeserializationConfiguration create() {
      return new LazyDeserializationConfiguration(enabled);
   }
   
}
