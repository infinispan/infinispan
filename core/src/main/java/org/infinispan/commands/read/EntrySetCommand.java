package org.infinispan.commands.read;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.RemovableIterator;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.DataContainerRemoveIterator;
import org.infinispan.util.EntryWrapper;

/**
 * Command implementation for {@link java.util.Map#entrySet()} functionality.
 *
 * @author Galder Zamarreño
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author William Burns
 * @since 4.0
 */
public class EntrySetCommand<K, V> extends AbstractLocalCommand implements VisitableCommand {
   private final Cache<K, V> cache;

   public EntrySetCommand(Cache<K, V> cache, long flagsBitSet) {
      setFlagsBitSet(flagsBitSet);
      cache = AbstractDelegatingCache.unwrapCache(cache);
      if (flagsBitSet != EnumUtil.EMPTY_BIT_SET) {
         this.cache = cache.getAdvancedCache().withFlags(EnumUtil.enumArrayOf(flagsBitSet, Flag.class));
      } else {
         this.cache = cache;
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEntrySetCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<CacheEntry<K, V>> perform(InvocationContext ctx) throws Throwable {
      Object lockOwner = ctx.getLockOwner();
      if (ctx.getLockOwner() != null) {
         return new BackingEntrySet<>(cache.getAdvancedCache().lockAs(lockOwner));
      }
      return new BackingEntrySet<>(cache);
   }

   @Override
   public String toString() {
      return "EntrySetCommand{" +
            "cache=" + cache.getName() +
            '}';
   }

   static class BackingEntrySet<K, V> extends AbstractCloseableIteratorCollection<CacheEntry<K, V>, K, V>
         implements CacheSet<CacheEntry<K, V>> {

      BackingEntrySet(Cache cache) {
         super(cache);
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         Iterator<CacheEntry<K, V>> iterator = new DataContainerRemoveIterator<>(cache);
         RemovableIterator<CacheEntry<K, V>> removableIterator = new RemovableIterator<>(iterator, e -> cache.remove(e.getKey(), e.getValue()));
         return Closeables.iterator(new IteratorMapper<>(removableIterator, e -> new EntryWrapper<>(cache, e)));
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         DataContainer<K, V> dc = cache.getAdvancedCache().getDataContainer();
         return Closeables.spliterator(Closeables.iterator(new DataContainerRemoveIterator<>(cache, dc)), dc.sizeIncludingExpired(),
                 Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
      }

      @Override
      public int size() {
         return cache.getAdvancedCache().getDataContainer().size();
      }

      @Override
      public boolean contains(Object o) {
         Map.Entry entry = toEntry(o);
         if (entry != null) {
            V value = cache.get(entry.getKey());
            return value != null && value.equals(entry.getValue());
         }
         return false;
      }

      @Override
      public boolean remove(Object o) {
         Map.Entry entry = toEntry(o);
         return entry != null && cache.remove(entry.getKey(), entry.getValue());
      }

      @Override
      public boolean add(CacheEntry<K, V> internalCacheEntry) {
         /**
          * {@link Map#entrySet()} defines no support for add or addAll methods
          */
         throw new UnsupportedOperationException();
      }

      private Map.Entry<K, V> toEntry(Object obj) {
         if (obj instanceof Map.Entry) {
            return (Map.Entry) obj;
         } else {
            return null;
         }
      }

      private ToIntFunction<Object> getSegmentMapper(Cache<K, V> cache) {
         DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
         if (dm != null) {
            return dm.getCacheTopology()::getSegment;
         }
         return null;
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, getSegmentMapper(cache),
                 () -> super.stream()), false, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, getSegmentMapper(cache),
                 () -> super.stream()), true, cache.getAdvancedCache().getComponentRegistry());
      }
   }
}
