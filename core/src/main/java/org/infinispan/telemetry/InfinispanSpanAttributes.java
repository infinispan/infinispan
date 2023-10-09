package org.infinispan.telemetry;

import java.util.Objects;
import java.util.Optional;

import org.infinispan.configuration.cache.Configuration;

public class InfinispanSpanAttributes {

   private final String cacheName;
   private final Configuration cacheConfiguration;
   private final SpanCategory category;

   private InfinispanSpanAttributes(String cacheName, Configuration cacheConfiguration, SpanCategory category) {
      this.cacheName = cacheName;
      this.cacheConfiguration = cacheConfiguration;
      this.category = category;
   }

   public Optional<String> cacheName() {
      return Optional.ofNullable(cacheName);
   }

   public SpanCategory category() {
      return category;
   }

   public boolean isCategoryDisabled() {
      if (cacheConfiguration == null) {
         return false;
      }

      return !cacheConfiguration.tracing().enabled(category);
   }

   public static class Builder {
      private String cacheName;
      private Configuration cacheConfiguration;
      private SpanCategory category;

      public Builder(SpanCategory category) {
         // category is mandatory
         this.category = Objects.requireNonNull(category);
      }

      public Builder withCache(String cacheName, Configuration cacheConfiguration) {
         this.cacheName = cacheName;
         this.cacheConfiguration = cacheConfiguration;
         return this;
      }

      public InfinispanSpanAttributes build() {
         return new InfinispanSpanAttributes(cacheName, cacheConfiguration, category);
      }
   }
}
