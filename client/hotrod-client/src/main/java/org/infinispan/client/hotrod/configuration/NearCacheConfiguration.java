package org.infinispan.client.hotrod.configuration;

import java.util.regex.Pattern;

import org.infinispan.client.hotrod.near.DefaultNearCacheFactory;
import org.infinispan.client.hotrod.near.NearCacheFactory;

public class NearCacheConfiguration {
   // TODO: Consider an option to configure key equivalence function for near cache (e.g. for byte arrays)
   private final NearCacheMode mode;
   private final int maxEntries;
   private final Pattern cacheNamePattern;
   private final NearCacheFactory nearCacheFactory;

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries) {
      this(mode, maxEntries, null, DefaultNearCacheFactory.INSTANCE);
   }

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, Pattern cacheNamePattern, NearCacheFactory nearCacheFactory) {
      this.mode = mode;
      this.maxEntries = maxEntries;
      this.cacheNamePattern = cacheNamePattern;
      this.nearCacheFactory = nearCacheFactory;
   }

   public int maxEntries() {
      return maxEntries;
   }

   public NearCacheMode mode() {
      return mode;
   }

   public Pattern cacheNamePattern() {
      return cacheNamePattern;
   }

   public NearCacheFactory nearCacheFactory() {
      return nearCacheFactory;
   }

   @Override
   public String toString() {
      return "NearCacheConfiguration{" +
            "mode=" + mode +
            ", maxEntries=" + maxEntries +
            ", cacheNamePattern=" + cacheNamePattern +
            ", nearCacheFactory=" + nearCacheFactory +
            '}';
   }
}
