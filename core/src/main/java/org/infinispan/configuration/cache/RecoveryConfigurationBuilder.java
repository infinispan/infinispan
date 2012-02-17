package org.infinispan.configuration.cache;


/**
 * Defines recovery configuration for the cache.
 * 
 * @author pmuir
 * 
 */
public class RecoveryConfigurationBuilder extends AbstractTransportConfigurationChildBuilder<RecoveryConfiguration> {

   private boolean enabled = false;
   private String recoveryInfoCacheName = "__recoveryInfoCacheName__";

   RecoveryConfigurationBuilder(TransactionConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enable recovery for this cache
    */
   public RecoveryConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   /**
    * Disable recovery for this cache
    */
   public RecoveryConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   /**
    * Enable recovery for this cache
    */
   public RecoveryConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Sets the name of the cache where recovery related information is held. If not specified
    * defaults to a cache named {@link RecoveryConfiguration#DEFAULT_RECOVERY_INFO_CACHE}
    */
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

   @Override
   public RecoveryConfigurationBuilder read(RecoveryConfiguration template) {
      this.enabled = template.enabled();
      this.recoveryInfoCacheName = template.recoveryInfoCacheName();

      return this;
   }

   @Override
   public String toString() {
      return "RecoveryConfigurationBuilder{" +
            "enabled=" + enabled +
            ", recoveryInfoCacheName='" + recoveryInfoCacheName + '\'' +
            '}';
   }

}
