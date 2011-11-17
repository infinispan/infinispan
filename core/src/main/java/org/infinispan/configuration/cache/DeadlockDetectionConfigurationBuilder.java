package org.infinispan.configuration.cache;

/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfigurationBuilder extends AbstractConfigurationChildBuilder<DeadlockDetectionConfiguration> {

   private boolean enabled;
   private long spinDuration;
   
   DeadlockDetectionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public DeadlockDetectionConfigurationBuilder spinDuration(long l) {
      this.spinDuration = l;
      return this;
   }
   
   public DeadlockDetectionConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public DeadlockDetectionConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   public DeadlockDetectionConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   DeadlockDetectionConfiguration create() {
      return new DeadlockDetectionConfiguration(enabled, spinDuration);
   }
   
}
