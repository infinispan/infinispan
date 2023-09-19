package org.infinispan.server.iteration.list;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.iteration.IterationInitializationContext;
import org.infinispan.stream.impl.local.AbstractLocalCacheStream;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ListIterationInitializationContext implements IterationInitializationContext {
   private List<Map.Entry<Object, Object>> source;

   public ListIterationInitializationContext(List<Map.Entry<Object, Object>> source) {
      this.source = source;
   }

   @Override
   public AbstractLocalCacheStream.StreamSupplier<CacheEntry<Object, Object>,
         Stream<CacheEntry<Object, Object>>> getBaseStream() {
      return new ListStreamSupplier(source);
   }

   public static ListIterationInitializationContext withSource(List<Map.Entry<Object, Object>> source) {
      return new ListIterationInitializationContext(source);
   }
}
