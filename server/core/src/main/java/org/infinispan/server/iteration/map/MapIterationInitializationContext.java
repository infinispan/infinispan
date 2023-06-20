package org.infinispan.server.iteration.map;

import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.iteration.IterationInitializationContext;
import org.infinispan.stream.impl.local.AbstractLocalCacheStream;

public class MapIterationInitializationContext implements IterationInitializationContext {
   private final Map<Object, Object> source;

   public MapIterationInitializationContext(Map<Object, Object> source) {
      this.source = source;
   }

   @Override
   public AbstractLocalCacheStream.StreamSupplier<CacheEntry<Object, Object>, Stream<CacheEntry<Object, Object>>> getBaseStream() {
      return new MapStreamSupplier(source);
   }

   public static MapIterationInitializationContext withSource(Map<?, ?> source) {
      return new MapIterationInitializationContext((Map<Object, Object>) source);
   }
}
