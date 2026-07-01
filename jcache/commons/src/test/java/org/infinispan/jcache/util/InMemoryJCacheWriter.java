package org.infinispan.jcache.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;

public class InMemoryJCacheWriter<K, V> implements CacheWriter<K, V>, Serializable {

   private final ConcurrentMap<K, V> store = new ConcurrentHashMap<>();
   private final LongAdder writeCount = new LongAdder();
   private final LongAdder deleteCount = new LongAdder();

   @Override
   public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
      store.put(entry.getKey(), entry.getValue());
      writeCount.increment();
   }

   @Override
   public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
      for (Cache.Entry<? extends K, ? extends V> entry : entries) {
         write(entry);
      }
   }

   @Override
   public void delete(Object key) throws CacheWriterException {
      store.remove(key);
      deleteCount.increment();
   }

   @Override
   public void deleteAll(Collection<?> keys) throws CacheWriterException {
      for (Object key : keys) {
         delete(key);
      }
   }

   public long getWriteCount() {
      return writeCount.longValue();
   }

   public long getDeleteCount() {
      return deleteCount.longValue();
   }

   public V get(K key) {
      return store.get(key);
   }

   public int size() {
      return store.size();
   }
}
