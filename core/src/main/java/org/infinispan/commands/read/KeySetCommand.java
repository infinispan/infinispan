package org.infinispan.commands.read;

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
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.impl.local.LocalKeyCacheStream;
import org.infinispan.util.DataContainerRemoveIterator;

import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

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

   private static class BackingKeySet<K, V> extends AbstractCloseableIteratorCollection<K, K, V> implements CacheSet<K> {

      public BackingKeySet(Cache<K, V> cache) {
         super(cache);
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new EntryToKeyIterator(new DataContainerRemoveIterator<>(cache));
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return Closeables.spliterator(iterator(), cache.getAdvancedCache().getDataContainer().size(),
                 Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
      }

      @Override
      public int size() {
         return cache.getAdvancedCache().getDataContainer().size();
      }

      @Override
      public boolean contains(Object o) {
         return cache.containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return cache.remove(o) != null;
      }

      @Override
      public CacheStream<K> stream() {
         DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
         return new LocalKeyCacheStream<>(cache, false,
                 dm != null ? dm.getConsistentHash() : null,
                 () -> {
                    DataContainer<K, V> dataContainer = cache.getAdvancedCache().getDataContainer();
                    Spliterator<CacheEntry<K, V>> spliterator = Spliterators.spliterator(
                            new DataContainerRemoveIterator<>(cache, dataContainer), dataContainer.size(),
                            Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
                    return StreamSupport.stream(spliterator, false);
                 }, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<K> parallelStream() {
         DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
         return new LocalKeyCacheStream<>(cache, true,
                 dm != null ? dm.getConsistentHash() : null,
                 () -> {
                    DataContainer<K, V> dataContainer = cache.getAdvancedCache().getDataContainer();
                    Spliterator<CacheEntry<K, V>> spliterator = Spliterators.spliterator(
                            new DataContainerRemoveIterator<>(cache, dataContainer), dataContainer.size(),
                            Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
                    return StreamSupport.stream(spliterator, true);
                 }, cache.getAdvancedCache().getComponentRegistry());
      }
   }

   private static class EntryToKeyIterator<K, V> implements CloseableIterator<K> {

      private final Iterator<CacheEntry<K, V>> iterator;

      public EntryToKeyIterator(Iterator<CacheEntry<K, V>> iterator) {
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
         // Do nothing as we can't close regular iterator
      }
   }
}
