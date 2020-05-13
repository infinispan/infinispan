package org.infinispan.client.hotrod.impl;

import java.util.AbstractSet;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Stream;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RemovableCloseableIterator;

class RemoteCacheEntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> implements CloseableIteratorSet<Map.Entry<K, V>> {
   private final InternalRemoteCache<K, V> remoteCache;
   private final IntSet segments;

   public RemoteCacheEntrySet(InternalRemoteCache<K, V> remoteCache, IntSet segments) {
      this.remoteCache = remoteCache;
      this.segments = segments;
   }

   @Override
   public CloseableIterator<Map.Entry<K, V>> iterator() {
      return new RemovableCloseableIterator<>(remoteCache.entryIterator(segments), this::remove);
   }

   @Override
   public CloseableSpliterator<Map.Entry<K, V>> spliterator() {
      return Closeables.spliterator(iterator(), Long.MAX_VALUE, Spliterator.NONNULL | Spliterator.CONCURRENT);
   }

   @Override
   public Stream<Map.Entry<K, V>> stream() {
      return Closeables.stream(spliterator(), false);
   }

   @Override
   public Stream<Map.Entry<K, V>> parallelStream() {
      return Closeables.stream(spliterator(), true);
   }

   @Override
   public int size() {
      return remoteCache.size();
   }

   @Override
   public void clear() {
      remoteCache.clear();
   }

   @Override
   public boolean contains(Object o) {
      if (!(o instanceof Map.Entry)) {
         return false;
      }
      //noinspection unchecked
      Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
      V value = remoteCache.get(entry.getKey());
      return value != null && value.equals(entry.getValue());
   }

   @Override
   public boolean remove(Object o) {
      if (!(o instanceof Map.Entry)) {
         return false;
      }
      //noinspection unchecked
      Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
      return remoteCache.removeEntry(entry);
   }
}
