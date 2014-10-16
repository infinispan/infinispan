package org.infinispan.commands.read;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.NullValueConverter;

import java.util.Set;

/**
 * Command implementation for {@link java.util.Map#keySet()} functionality.
 *
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author William Burns
 * @since 4.0
 */
public class KeySetCommand<K, V> extends AbstractLocalCommand implements VisitableCommand {
   private final Cache<K, V> cache;

   public KeySetCommand(Cache<K, V> cache, Set<Flag> flags) {
      setFlags(flags);
      if (flags != null) {
         this.cache = cache.getAdvancedCache().withFlags(flags.toArray(new Flag[flags.size()]));
      } else {
         this.cache = cache;
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitKeySetCommand(ctx, this);
   }

   @Override
   public Set<K> perform(InvocationContext ctx) throws Throwable {
      return new BackingKeySet<>(cache);
   }

   @Override
   public String toString() {
      return "KeySetCommand{" +
            "cache=" + cache.getName() +
            '}';
   }

   private static class BackingKeySet<K, V> extends AbstractCloseableIteratorCollection<K, K, V> implements CloseableIteratorSet<K> {

      public BackingKeySet(Cache<K, V> cache) {
         super(cache);
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new EntryToKeyIterator(cache.getAdvancedCache().filterEntries(AcceptAllKeyValueFilter.getInstance())
                                             .converter(NullValueConverter.getInstance()).iterator());
      }

      @Override
      public boolean contains(Object o) {
         return cache.containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return cache.remove(o) != null;
      }
   }

   private static class EntryToKeyIterator<K> implements CloseableIterator<K> {

      private final CloseableIterator<CacheEntry<K, Void>> iterator;

      public EntryToKeyIterator(CloseableIterator<CacheEntry<K, Void>> iterator) {
         this.iterator = iterator;
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public K next() {
         return iterator.next().getKey();
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
