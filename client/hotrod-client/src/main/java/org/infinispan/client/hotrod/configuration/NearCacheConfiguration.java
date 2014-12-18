package org.infinispan.client.hotrod.configuration;

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
}
