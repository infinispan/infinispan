package org.infinispan.commons.util;

import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.util.AbstractDelegatingConcurrentMap;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * ConcurrentMap implementation backed by Caffeine that also implements {@link ParallelIterableMap}.
 * @author wburns
 * @since 9.0
 */
public class CaffeineConcurrentMap<K, V> extends AbstractDelegatingConcurrentMap<K, V> implements ParallelIterableMap<K, V> {
   private final Cache<K, V> cache;
   private final ConcurrentMap<K, V> concurrentMap;

   public CaffeineConcurrentMap(Cache<K, V> cache) {
      this.cache = cache;
      this.concurrentMap = cache.asMap();
   }

   public Cache<K, V> getCache() {
      return cache;
   }

   @Override
   protected ConcurrentMap<K, V> delegate() {
      return concurrentMap;
   }

   @Override
   public void forEach(long parallelismThreshold, BiConsumer<? super K, ? super V> action) throws InterruptedException {
      concurrentMap.forEach(action);
   }
}
