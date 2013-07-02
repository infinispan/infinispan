package org.infinispan.configuration.cache;

import org.infinispan.commons.marshall.Marshaller;

/**
 * Compatibility mode configuration
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class CompatibilityModeConfiguration {

   private final boolean enabled;
   private final Marshaller marshaller;

   CompatibilityModeConfiguration(boolean enabled, Marshaller marshaller) {
      this.enabled = enabled;
      this.marshaller = marshaller;
   }

   public boolean enabled() {
      return enabled;
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   @Override
   public String toString() {
      return "CompatibilityModeConfiguration{" +
            "enabled=" + enabled +
            ", marshaller=" + marshaller +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CompatibilityModeConfiguration that = (CompatibilityModeConfiguration) o;

      if (enabled != that.enabled) return false;
      if (marshaller != null ? !marshaller.equals(that.marshaller) : that.marshaller != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (marshaller != null ? marshaller.hashCode() : 0);
      return result;
   }
}
