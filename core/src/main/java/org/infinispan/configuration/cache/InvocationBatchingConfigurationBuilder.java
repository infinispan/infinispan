package org.infinispan.configuration.cache;

public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder<InvocationBatchingConfiguration> {

   private boolean enabled = false;
   
   InvocationBatchingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   public InvocationBatchingConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public InvocationBatchingConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   InvocationBatchingConfiguration create() {
      return new InvocationBatchingConfiguration(enabled);
   }
   
   @Override
   public InvocationBatchingConfigurationBuilder read(InvocationBatchingConfiguration template) {
      this.enabled = template.enabled();
      
      return this;
   }
   
}
