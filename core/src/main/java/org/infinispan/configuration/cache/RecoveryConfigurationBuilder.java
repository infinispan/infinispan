package org.infinispan.configuration.cache;


public class RecoveryConfigurationBuilder extends AbstractTransportConfigurationChildBuilder<RecoveryConfiguration> {

   private boolean enabled;
   private String recoveryInfoCacheName;

   RecoveryConfigurationBuilder(TransactionConfigurationBuilder builder) {
      super(builder);
   }

   public RecoveryConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public RecoveryConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   public RecoveryConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public RecoveryConfigurationBuilder recoveryInfoCacheName(String recoveryInfoName) {
      this.recoveryInfoCacheName = recoveryInfoName;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub

   }

   @Override
   RecoveryConfiguration create() {
      return new RecoveryConfiguration(enabled, recoveryInfoCacheName);
   }

}
