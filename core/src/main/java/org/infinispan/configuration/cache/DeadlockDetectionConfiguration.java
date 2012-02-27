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
   
   /**
    * Whether deadlock detection is enabled or disabled
    */
   public boolean enabled() {
      return enabled;
   }

   @Override
   public String toString() {
      return "DeadlockDetectionConfiguration{" +
            "enabled=" + enabled +
            ", spinDuration=" + spinDuration +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DeadlockDetectionConfiguration that = (DeadlockDetectionConfiguration) o;

      if (enabled != that.enabled) return false;
      if (spinDuration != that.spinDuration) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (int) (spinDuration ^ (spinDuration >>> 32));
      return result;
   }

}
