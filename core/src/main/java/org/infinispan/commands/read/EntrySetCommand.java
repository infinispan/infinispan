package org.infinispan.commands.read;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.IteratorAsCloseableIterator;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.AcceptAllKeyValueFilter;

import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;

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
   public Set<CacheEntry<K, V>> perform(InvocationContext ctx) throws Throwable {
      return new BackingEntrySet<>(cache);
   }

   @Override
   public String toString() {
      return "EntrySetCommand{" +
            "cache=" + cache.getName() +
            '}';
   }

   private static class BackingEntrySet<K, V> extends AbstractCloseableIteratorCollection<CacheEntry<K, V>, K, V>
         implements CloseableIteratorSet<CacheEntry<K, V>> {

      private BackingEntrySet(Cache cache) {
         super(cache);
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         CloseableIterator<CacheEntry<K, V>> iterator = new IteratorAsCloseableIterator<>(
                 cache.getAdvancedCache().getDataContainer().iterator());
         return new EntryWrapperIterator<>(cache, iterator);
      }

      @Override
      public Spliterator<CacheEntry<K, V>> spliterator() {
         // This is a lame hack because Spliterator<InternalCacheEntry<K, V>> cannot be case to Spliterator<CacheEntry<K, V>>
         Spliterator<CacheEntry<K, V>> spliterator = Spliterators.spliterator(
                 cache.getAdvancedCache().getDataContainer().iterator(), Long.MAX_VALUE, Spliterator.CONCURRENT |
                         Spliterator.NONNULL | Spliterator.DISTINCT);
         return spliterator;
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
         V value = cache.put(internalCacheEntry.getKey(), internalCacheEntry.getValue());
         // If the value was already there we can treat as if it wasn't added
         return value != null && value.equals(internalCacheEntry.getValue());
      }

      private Map.Entry<K, V> toEntry(Object obj) {
         if (obj instanceof Map.Entry) {
            return (Map.Entry) obj;
         } else {
            return null;
         }
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
      private final CloseableIterator<CacheEntry<K, V>> iterator;

      public EntryWrapperIterator(Cache<K, V> cache, CloseableIterator<CacheEntry<K, V>> iterator) {
         this.cache = cache;
         this.iterator = iterator;
      }

      @Override
      public void close() {
         iterator.close();
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
