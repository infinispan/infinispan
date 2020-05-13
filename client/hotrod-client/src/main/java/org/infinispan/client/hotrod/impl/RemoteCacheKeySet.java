package org.infinispan.client.hotrod.impl;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Spliterator;
import java.util.stream.Stream;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RemovableCloseableIterator;

class RemoteCacheKeySet<K> extends AbstractSet<K> implements CloseableIteratorSet<K> {
   private final InternalRemoteCache<K, ?> remoteCache;
   private final IntSet segments;

   RemoteCacheKeySet(InternalRemoteCache<K, ?> remoteCache, IntSet segments) {
      this.remoteCache = remoteCache;
      this.segments = segments;
   }

   @Override
   public CloseableIterator<K> iterator() {
      CloseableIterator<K> keyIterator = remoteCache.keyIterator(segments);
      return new RemovableCloseableIterator<>(keyIterator, this::remove);
   }

   @Override
   public CloseableSpliterator<K> spliterator() {
      return Closeables.spliterator(iterator(), Long.MAX_VALUE, Spliterator.NONNULL | Spliterator.CONCURRENT |
            Spliterator.DISTINCT);
   }

   @Override
   public Stream<K> stream() {
      return Closeables.stream(spliterator(), false);
   }

   @Override
   public Stream<K> parallelStream() {
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
      return remoteCache.containsKey(o);
   }

   @Override
   public boolean remove(Object o) {
      return remoteCache.remove(o) != null;
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      boolean removedSomething = false;
      for (Object key : c) {
         removedSomething |= remove(key);
      }
      return removedSomething;
   }
}
