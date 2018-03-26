package org.infinispan.commands.read;

import java.util.AbstractCollection;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.CloseableSpliteratorMapper;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.DataContainerRemoveIterator;

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

   public KeySetCommand(Cache<K, V> cache, long flagsBitSet) {
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
      return visitor.visitKeySetCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<K> perform(InvocationContext ctx) throws Throwable {
      // We have to check for the flags when we do perform - as interceptor could change this while going down
      // the stack
      boolean isRemoteIteration = EnumUtil.containsAny(getFlagsBitSet(), FlagBitSets.REMOTE_ITERATION);
      return new BackingKeySet<>(cache, isRemoteIteration);
   }

   @Override
   public String toString() {
      return "KeySetCommand{" +
            "cache=" + cache.getName() +
            ", flags=" + printFlags() +
            '}';
   }

   private static class BackingKeySet<K, V> extends AbstractCollection<K> implements CacheSet<K> {
      private final Cache<K, V> cache;
      private final boolean isRemoteIteration;

      BackingKeySet(Cache<K, V> cache, boolean isRemoteIteration) {
         this.cache = cache;
         this.isRemoteIteration = isRemoteIteration;
      }

      @Override
      public CloseableIterator<K> iterator() {
         if (isRemoteIteration) {
            // Don't add the extra wrapping for removal
            return new CloseableIteratorMapper<>(cache.getAdvancedCache().getDataContainer().iterator(), Map.Entry::getKey);
         }
         return new CloseableIteratorMapper<>(new DataContainerRemoveIterator<>(cache), Map.Entry::getKey);
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         DataContainer<K, V> dc = cache.getAdvancedCache().getDataContainer();

         // Spliterator doesn't support remove so just return it without wrapping
         return new CloseableSpliteratorMapper<>(dc.spliterator(), Map.Entry::getKey);
      }

      @Override
      public int size() {
         return cache.getAdvancedCache().getDataContainer().size();
      }

      @Override
      public boolean contains(Object o) {
         return cache.getAdvancedCache().getDataContainer().containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return cache.remove(o) != null;
      }

      @Override
      public CacheStream<K> stream() {
         DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
         return new LocalCacheStream<>(new KeyStreamSupplier<>(cache, dm != null ? dm.getCacheTopology()::getSegment : null,
               () -> StreamSupport.stream(spliterator(), false)), false, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<K> parallelStream() {
         DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
         return new LocalCacheStream<>(new KeyStreamSupplier<>(cache, dm != null ? dm.getCacheTopology()::getSegment : null,
               () -> StreamSupport.stream(spliterator(), false)), true, cache.getAdvancedCache().getComponentRegistry());
      }
   }
}
