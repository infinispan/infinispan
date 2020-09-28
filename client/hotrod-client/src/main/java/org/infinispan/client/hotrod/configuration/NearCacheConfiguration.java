package org.infinispan.client.hotrod.configuration;

import java.util.regex.Pattern;

public class NearCacheConfiguration {
   // TODO: Consider an option to configure key equivalence function for near cache (e.g. for byte arrays)
   private final NearCacheMode mode;
   private final int maxEntries;
   private final boolean bloomFilter;
   private final Pattern cacheNamePattern;

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, boolean bloomFilterOptimization) {
      this(mode, maxEntries, bloomFilterOptimization, null);
   }

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, boolean bloomFilter, Pattern cacheNamePattern) {
      this.mode = mode;
      this.maxEntries = maxEntries;
      this.bloomFilter = bloomFilter;
      this.cacheNamePattern = cacheNamePattern;
   }

   public int maxEntries() {
      return maxEntries;
   }

   public NearCacheMode mode() {
      return mode;
   }

   public boolean bloomFilter() {
      return bloomFilter;
   }

   public Pattern cacheNamePattern() {
      return cacheNamePattern;
   }

   @Override
   public String toString() {
      return "NearCacheConfiguration{" +
            "mode=" + mode +
            ", maxEntries=" + maxEntries +
            ", bloomFilter=" + bloomFilter +
            ", cacheNamePattern=" + cacheNamePattern +
            '}';
   }
}
