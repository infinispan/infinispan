package org.infinispan.telemetry.impl;

import java.util.EnumMap;
import java.util.Map;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.SpanCategory;

@Scope(Scopes.NAMED_CACHE)
public class CacheSpanAttribute {

   private final Map<SpanCategory, InfinispanSpanAttributes> cachedSpanAttributes;

   public CacheSpanAttribute(String cacheName, Configuration configuration) {
      cachedSpanAttributes = new EnumMap<>(SpanCategory.class);
      for (var category : SpanCategory.values()) {
         cachedSpanAttributes.put(category, new InfinispanSpanAttributes.Builder(category).withCache(cacheName, configuration).build());
      }
   }

   public InfinispanSpanAttributes getAttributes(SpanCategory category) {
      return cachedSpanAttributes.get(category);
   }
}
