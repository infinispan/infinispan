package org.jboss.as.clustering.infinispan.cs.factory;

import java.util.function.Predicate;

import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.reactivestreams.Publisher;

public class CustomStoreWithoutConfiguration implements AdvancedCacheLoader<Object,Object> {
   @Override
   public int size() {
      return 0;
   }

   @Override
   public void init(InitializationContext ctx) {
   }

   @Override
   public MarshallableEntry<Object, Object> loadEntry(Object key) {
      return null;
   }

   @Override
   public Publisher<MarshallableEntry<Object, Object>> entryPublisher(Predicate<? super Object> filter, boolean fetchValue, boolean fetchMetadata) {
      return null;
   }

   @Override
   public boolean contains(Object key) {
      return false;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }
}
