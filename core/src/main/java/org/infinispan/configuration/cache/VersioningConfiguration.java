package org.infinispan.configuration.cache;

/**
 * This configuration element controls whether entries are versioned.  Versioning is necessary, for example, when
 * using optimistic transactions in a clustered environment, to be able to perform write-skew checks.
 */
public class VersioningConfiguration {
   private final boolean enabled;
   private final VersioningScheme scheme;

   VersioningConfiguration(boolean enabled, VersioningScheme scheme) {
      this.enabled = enabled;
      this.scheme = scheme;
   }

   public boolean enabled() {
      return enabled;
   }

   public VersioningScheme scheme() {
      return scheme;
   }

   @Override
   public String toString() {
      return "VersioningConfiguration{" +
            "enabled=" + enabled +
            ", scheme=" + scheme +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      VersioningConfiguration that = (VersioningConfiguration) o;

      if (enabled != that.enabled) return false;
      if (scheme != that.scheme) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (scheme != null ? scheme.hashCode() : 0);
      return result;
   }

}
