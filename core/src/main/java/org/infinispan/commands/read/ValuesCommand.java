package org.infinispan.commands.read;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.AcceptAllKeyValueFilter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Command implementation for {@link java.util.Map#values()} functionality.
 *
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author William Burns
 * @since 4.0
 */
public class ValuesCommand<K, V> extends AbstractLocalCommand implements VisitableCommand {
   private final Cache<K, V> cache;

   public ValuesCommand(Cache<K, V> cache, Set<Flag> flags) {
      setFlags(flags);
      if (flags != null) {
         this.cache = cache.getAdvancedCache().withFlags(flags.toArray(new Flag[flags.size()]));
      } else {
         this.cache = cache;
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitValuesCommand(ctx, this);
   }

   @Override
   public Collection<V> perform(InvocationContext ctx) throws Throwable {
      return new BackingValuesCollection<>(cache);
   }

   @Override
   public String toString() {
      return "ValuesCommand{" +
            "cache=" + cache.getName() +
            '}';
   }

   private static class BackingValuesCollection<K, V> extends AbstractCloseableIteratorCollection<V, K, V> {

      public BackingValuesCollection(Cache<K, V> cache) {
         super(cache);
      }

      @Override
      public CloseableIterator<V> iterator() {
         return new EntryToValueIterator(cache.getAdvancedCache().filterEntries(AcceptAllKeyValueFilter.getInstance()).iterator());
      }

      @Override
      public boolean contains(Object o) {
         // We don't support null values
         if (o == null) {
            throw new NullPointerException();
         }
         try (CloseableIterator<V> it = iterator()) {
            while (it.hasNext())
               if (o.equals(it.next()))
                  return true;
            return false;
         }
      }

      @Override
      public boolean containsAll(Collection<?> c) {
         // The AbstractCollection implementation calls contains for each element.  Instead we want to call the iterator
         // only once so we have a special implementation.
         if (c.size() > 0) {
            Set<?> set = new HashSet<>(c);
            try (CloseableIterator<V> it = iterator()) {
               while (!set.isEmpty() && it.hasNext()) {
                  set.remove(it.next());
               }
            }
            return set.isEmpty();
         }
         return true;
      }

      @Override
      public boolean remove(Object o) {
         try (CloseableIterator<V> it = iterator()) {
            while (it.hasNext()) {
               if (o.equals(it.next())) {
                  it.remove();
                  return true;
               }
            }
            return false;
         }
      }
   }

   private static class EntryToValueIterator<V> implements CloseableIterator<V> {

      private final CloseableIterator<CacheEntry<?, V>> iterator;

      public EntryToValueIterator(CloseableIterator<CacheEntry<?, V>> iterator) {
         this.iterator = iterator;
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public V next() {
         return iterator.next().getValue();
      }

      @Override
      public void remove() {
         iterator.remove();
      }

      @Override
      public void close() {
         iterator.close();
      }
   }
}
