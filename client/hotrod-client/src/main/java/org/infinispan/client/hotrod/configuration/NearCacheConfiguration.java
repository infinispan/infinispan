package org.infinispan.client.hotrod.configuration;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;

import java.util.Properties;

public class NearCacheConfiguration {
   // TODO: Consider an option to configure key equivalence function for near cache (e.g. for byte arrays)
   private final NearCacheMode mode;
   private final int maxEntries;

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries) {
      this.mode = mode;
      this.maxEntries = maxEntries;
   }

   public int maxEntries() {
      return maxEntries;
   }

   public NearCacheMode mode() {
      return mode;
   }

   @Override
   public String toString() {
      return "NearCacheConfiguration{" +
            "mode=" + mode +
            ", maxEntries=" + maxEntries +
            '}';
   }

   void toProperties(Properties properties) {
      properties.setProperty(ConfigurationProperties.NEAR_CACHE_MODE, mode.name());
      properties.setProperty(ConfigurationProperties.NEAR_CACHE_MAX_ENTRIES, Integer.toString(maxEntries));
   }

}
