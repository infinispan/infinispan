package org.infinispan.client.hotrod.configuration;

import org.infinispan.client.hotrod.near.DefaultNearCacheFactory;
import org.infinispan.client.hotrod.near.NearCacheFactory;

public class NearCacheConfiguration {
   // TODO: Consider an option to configure key equivalence function for near cache (e.g. for byte arrays)
   private final NearCacheMode mode;
   private final int maxEntries;
   private final boolean bloomFilter;
   private final NearCacheFactory nearCacheFactory;

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, boolean bloomFilterOptimization) {
      this(mode, maxEntries, bloomFilterOptimization, DefaultNearCacheFactory.INSTANCE);
   }

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, boolean bloomFilter, NearCacheFactory nearCacheFactory) {
      this.mode = mode;
      this.maxEntries = maxEntries;
      this.bloomFilter = bloomFilter;
      this.nearCacheFactory = nearCacheFactory;
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

   public NearCacheFactory nearCacheFactory() {
      return nearCacheFactory;
   }

   @Override
   public String toString() {
      return "NearCacheConfiguration{" +
            "mode=" + mode +
            ", maxEntries=" + maxEntries +
            ", bloomFilter=" + bloomFilter +
            ", nearCacheFactory=" + nearCacheFactory +
            '}';
   }
}
