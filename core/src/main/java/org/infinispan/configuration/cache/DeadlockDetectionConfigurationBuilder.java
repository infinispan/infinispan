package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<DeadlockDetectionConfiguration> {

   private boolean enabled = false;
   private long spinDuration = TimeUnit.MILLISECONDS.toMillis(100);

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

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public DeadlockDetectionConfigurationBuilder spinDuration(long l, TimeUnit unit) {
      return spinDuration(unit.toMillis(l));
   }

   /**
    * Enable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   /**
    * Disable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   /**
    * Enable or disable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   public
   void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public
   DeadlockDetectionConfiguration create() {
      return new DeadlockDetectionConfiguration(enabled, spinDuration);
   }

   @Override
   public DeadlockDetectionConfigurationBuilder read(DeadlockDetectionConfiguration template) {
      this.enabled = template.enabled();
      this.spinDuration = template.spinDuration();

      return this;
   }

   @Override
   public String toString() {
      return "DeadlockDetectionConfigurationBuilder{" +
            "enabled=" + enabled +
            ", spinDuration=" + spinDuration +
            '}';
   }
}
