package org.infinispan.configuration.cache;


/**
 * Defines recovery configuration for the cache.
 * 
 * @author pmuir
 * 
 */
public class RecoveryConfiguration {

   public static final String DEFAULT_RECOVERY_INFO_CACHE = "__recoveryInfoCacheName__";

   private final boolean enabled;
   private final String recoveryInfoCacheName;

   RecoveryConfiguration(boolean enabled, String recoveryInfoCacheName) {
      this.enabled = enabled;
      this.recoveryInfoCacheName = recoveryInfoCacheName;
   }

   /**
    * Determines if recovery is enabled for the cache.
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * Sets the name of the cache where recovery related information is held. If not specified
    * defaults to a cache named {@link RecoveryConfiguration#DEFAULT_RECOVERY_INFO_CACHE}
    */
   public String recoveryInfoCacheName() {
      return recoveryInfoCacheName;
   }

   @Override
   public String toString() {
      return "RecoveryConfiguration{" +
            "enabled=" + enabled +
            ", recoveryInfoCacheName='" + recoveryInfoCacheName + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RecoveryConfiguration that = (RecoveryConfiguration) o;

      if (enabled != that.enabled) return false;
      if (recoveryInfoCacheName != null ? !recoveryInfoCacheName.equals(that.recoveryInfoCacheName) : that.recoveryInfoCacheName != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (recoveryInfoCacheName != null ? recoveryInfoCacheName.hashCode() : 0);
      return result;
   }

}
