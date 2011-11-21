package org.infinispan.configuration.cache;

/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfiguration {

   private final boolean enabled;
   private final long spinDuration;
   
   DeadlockDetectionConfiguration(boolean enabled, long spinDuration) {
      this.enabled = enabled;
      this.spinDuration = spinDuration;
   }
   
   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public long spinDuration() {
      return spinDuration;
   }
   
   public boolean enabled() {
      return enabled;
   }
   
}
