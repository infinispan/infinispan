package org.infinispan.configuration.cache;


@SuppressWarnings({"deprecation", "boxing"})
public class LegacyConfigurationAdaptor {
   private LegacyConfigurationAdaptor() {
      // Hide constructor
   }

   public static org.infinispan.config.Configuration adapt(org.infinispan.configuration.cache.Configuration config) {
      // No op.
      return null;
   }

   public static org.infinispan.configuration.cache.Configuration adapt(org.infinispan.config.Configuration legacy) {
      // No op.
      return null;
   }

}
