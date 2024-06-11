package org.infinispan.client.hotrod.impl;

import java.util.AbstractCollection;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.RemovableCloseableIterator;

class RemoteCacheValuesCollection<V> extends AbstractCollection<V> implements CloseableIteratorCollection<V> {
   private final InternalRemoteCache<?, V> remoteCache;
   private final IntSet segments;

   RemoteCacheValuesCollection(InternalRemoteCache<?, V> remoteCache, IntSet segments) {
      this.remoteCache = remoteCache;
      this.segments = segments;
   }

   @Override
   public CloseableIterator<V> iterator() {
      CloseableIterator<Map.Entry<?, V>> entryIterator = ((InternalRemoteCache) remoteCache).entryIterator(segments);
      return new IteratorMapper<>(new RemovableCloseableIterator<>(entryIterator, e -> remoteCache.remove(e.getKey(), e.getValue())),
            // Convert to V for user
            Map.Entry::getValue);
   }

   @Override
   public CloseableSpliterator<V> spliterator() {
      return Closeables.spliterator(iterator(), Long.MAX_VALUE, Spliterator.NONNULL | Spliterator.CONCURRENT);
   }

   @Override
   public Stream<V> stream() {
      return Closeables.stream(spliterator(), false);
   }

   @Override
   public Stream<V> parallelStream() {
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
      // TODO: This would be more efficient if done on the server as a task or separate protocol
      // Have to close the stream, just in case stream terminates early
      try (Stream<V> stream = stream()) {
         return stream.anyMatch(v -> Objects.deepEquals(v, o));
      }
   }

   // This method can terminate early so we have to make sure to close iterator
   @Override
   public boolean remove(Object o) {
      Objects.requireNonNull(o);
      try (CloseableIterator<V> iter = iterator()) {
         while (iter.hasNext()) {
            if (o.equals(iter.next())) {
               iter.remove();
               return true;
            }
         }
      }
      return false;
   }
}
