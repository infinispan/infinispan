package org.infinispan.commands.read;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.DataContainerRemoveIterator;

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

   public EntrySetCommand(Cache<K, V> cache, Set<Flag> flags) {
      setFlags(flags);
      if (flags != null) {
         this.cache = cache.getAdvancedCache().withFlags(flags.toArray(new Flag[flags.size()]));
      } else {
         this.cache = cache;
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEntrySetCommand(ctx, this);
   }

   @Override
   public boolean readsExistingValues() {
      return false;
   }

   @Override
   public Set<CacheEntry<K, V>> perform(InvocationContext ctx) throws Throwable {
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
         return new EntryWrapperIterator<>(cache, iterator);
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         DataContainer<K, V> dc = cache.getAdvancedCache().getDataContainer();
         return Closeables.spliterator(Closeables.iterator(new DataContainerRemoveIterator<>(cache, dc)), dc.size(),
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

      private ConsistentHash getConsistentHash(Cache<K, V> cache) {
         DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
         if (dm != null) {
            return dm.getReadConsistentHash();
         }
         return null;
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, getConsistentHash(cache),
                 () -> super.stream()), false, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, getConsistentHash(cache),
                 () -> super.stream()), true, cache.getAdvancedCache().getComponentRegistry());
      }
   }

   /**
    * Wrapper for iterator that produces CacheEntry instances that allow for updating the cache when
    * the cache entry's value is updated
    * @param <K> The key type
    * @param <V> The value type
    */
   private static class EntryWrapperIterator<K, V> implements CloseableIterator<CacheEntry<K, V>> {
      private final Cache<K, V> cache;
      private final Iterator<CacheEntry<K, V>> iterator;

      public EntryWrapperIterator(Cache<K, V> cache, Iterator<CacheEntry<K, V>> iterator) {
         this.cache = cache;
         this.iterator = iterator;
      }

      @Override
      public void close() {
         // Does nothing because data container iterator doesn't need to be closed
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public CacheEntry<K, V> next() {
         CacheEntry<K, V> entry = iterator.next();
         return new EntryWrapper<>(cache, entry);
      }

      @Override
      public void remove() {
         iterator.remove();
      }
   }

   /**
    * Wrapper for CacheEntry(s) that can be used to update the cache when it's value is set.
    * @param <K> The key type
    * @param <V> The value type
    */
   private static class EntryWrapper<K, V> extends ForwardingCacheEntry<K, V> {
      private final Cache<K, V> cache;
      private final CacheEntry<K, V> entry;

      public EntryWrapper(Cache<K, V> cache, CacheEntry<K, V> entry) {
         this.cache = cache;
         this.entry = entry;
      }

      @Override
      protected CacheEntry<K, V> delegate() {
         return entry;
      }

      @Override
      public V setValue(V value) {
         cache.put(entry.getKey(), value);
         return super.setValue(value);
      }
   }
}
