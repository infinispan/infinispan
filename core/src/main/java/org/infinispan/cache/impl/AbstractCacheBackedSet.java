package org.infinispan.cache.impl;

import static org.infinispan.context.InvocationContextFactory.UNBOUNDED;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.PublisherManagerFactory;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.transaction.impl.LocalTransaction;

/**
 * Entry or key set backed by a cache.
 *
 * @since 13.0
 */
public abstract class AbstractCacheBackedSet<K, V, E> implements CacheSet<E> {
   protected final CacheImpl<K, V> cache;
   protected final Object lockOwner;
   protected final long explicitFlags;

   private final int batchSize;

   private final ClusterPublisherManager<K, V> clusterPublisherManager;
   private final ClusterPublisherManager<K, V> localPublisherManager;
   private final Executor nonBlockingExecutor;

   public AbstractCacheBackedSet(CacheImpl<K, V> cache, Object lockOwner, long explicitFlags) {
      this.cache = cache;
      this.lockOwner = lockOwner;
      this.explicitFlags = explicitFlags;

      batchSize = cache.config.clustering().stateTransfer().chunkSize();

      clusterPublisherManager = cache.componentRegistry.getComponent(ClusterPublisherManager.class);
      localPublisherManager = cache.componentRegistry.getComponent(ClusterPublisherManager.class,
                                                                   PublisherManagerFactory.LOCAL_CLUSTER_PUBLISHER);
      nonBlockingExecutor = cache.componentRegistry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR);
   }

   @Override
   public int size() {
      return cache.size(explicitFlags);
   }

   @Override
   public boolean isEmpty() {
      return getStream(false).noneMatch(StreamMarshalling.alwaysTruePredicate());
   }

   @Override
   public abstract boolean contains(Object o);

   @Override
   public CloseableIterator<E> iterator() {
      CacheStream<E> stream = getStream(false);
      Iterator<E> iterator = stream.iterator();
      return new CloseableIterator<E>() {
         private E last;

         @Override
         public void close() {
            stream.close();
         }

         @Override
         public boolean hasNext() {
            return iterator.hasNext();
         }

         @Override
         public E next() {
            last = iterator.next();
            return wrapElement(last);
         }

         @Override
         public void remove() {
            Object key = extractKey(last);
            cache.remove(key, explicitFlags, decoratedWriteContextBuilder());
         }
      };
   }

   @Override
   public void forEach(Consumer<? super E> action) {
      try (CacheStream<E> stream = getStream(false)) {
         Iterator<E> iterator = stream.iterator();
         iterator.forEachRemaining(action);
      }
   }

   @Override
   public Object[] toArray() {
      return toArray(new Object[0]);
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return stream().toArray(n -> (T[]) Array.newInstance(a.getClass().getComponentType(), n));
   }

   /**
    * Adding new cache entries via a set is not allowed.
    *
    * <p>Please use {@link Cache#put(Object, Object)} etc.</p>
    */
   @Override
   public boolean add(E e) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(Object o) {
      Object key = entryToKeyFunction() != null ? extractKey(o) : o;
      V removedValue = cache.remove(key, explicitFlags, decoratedWriteContextBuilder());
      return removedValue != null;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      for (Object o : c) {
         if (!contains(o))
            return false;
      }
      return true;
   }

   /**
    * Adding new cache entries via a set is not allowed.
    *
    * <p>Please use {@link Cache#put(Object, Object)} etc.</p>
    */
   @Override
   public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      boolean modified = false;
      for (Object o : c) {
         modified |= remove(o);
      }
      return modified;
   }

   @Override
   public boolean removeIf(Predicate<? super E> filter) {
      Objects.requireNonNull(filter);
      boolean removed = false;
      try (CacheStream<E> stream = getStream(false)) {
         Iterator<E> iterator = stream.iterator();
         while (iterator.hasNext()) {
            E next = iterator.next();
            if (filter.test(next)) {
               Object key = extractKey(next);
               cache.remove(key, explicitFlags, decoratedWriteContextBuilder());
               removed = true;
            }
         }
      }
      return removed;
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return removeIf(e -> !c.contains(e));
   }

   @Override
   public void clear() {
      cache.clear(explicitFlags);
   }

   @Override
   public CloseableSpliterator<E> spliterator() {
      CacheStream<E> stream = getStream(false);
      return Closeables.spliterator(stream);
   }

   @Override
   public CacheStream<E> stream() {
      return getStream(false);
   }

   @Override
   public CacheStream<E> parallelStream() {
      return getStream(true);
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + "(" + cache + ')';
   }

   private CacheStream<E> getStream(boolean parallel) {
      ClusterPublisherManager<K, V> publisherManager;
      if (EnumUtil.containsAll(explicitFlags, FlagBitSets.CACHE_MODE_LOCAL)) {
         publisherManager = localPublisherManager;
      } else {
         publisherManager = clusterPublisherManager;
      }
      InvocationContext ctx = cache.invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      if (ctx.isInTxScope()) {
         // Register the cache transaction as a TM resource, so that it is cleaned up on commit
         // The EntrySetCommand/KeySetCommand invocations use a new context, so they may not enlist
         // E.g. when running from TxLockedStreamImpl.forEach
         TxInvocationContext txCtx = (TxInvocationContext)ctx;
         cache.txTable.enlist(txCtx.getTransaction(), (LocalTransaction) txCtx.getCacheTransaction());
      }
      if (lockOwner != null) {
         ctx.setLockOwner(lockOwner);
      }
      CacheStream<E> cacheStream =
            new DistributedCacheStream<>(cache.getCacheManager().getAddress(), parallel,
                                         ctx, explicitFlags, batchSize, nonBlockingExecutor,
                                         cache.componentRegistry, entryToKeyFunction(),
                                         publisherManager);
      return cacheStream.timeout(cache.config.clustering().remoteTimeout(), TimeUnit.MILLISECONDS);
   }

   protected ContextBuilder decoratedWriteContextBuilder() {
      return lockOwner == null ? cache.defaultContextBuilderForWrite() : this::createContextWithLockOwner;
   }

   private InvocationContext createContextWithLockOwner(int numKeys) {
      InvocationContext ctx = cache.defaultContextBuilderForWrite().create(numKeys);
      ctx.setLockOwner(lockOwner);
      return ctx;
   }

   protected abstract Function<Map.Entry<K, V>, ?> entryToKeyFunction();

   /**
    * Extract the key from a set element.
    */
   protected abstract Object extractKey(Object e);

   /**
    * Wrap the element if needed
    */
   protected abstract E wrapElement(E e);
}
