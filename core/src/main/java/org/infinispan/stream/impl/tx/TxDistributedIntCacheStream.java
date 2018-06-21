package org.infinispan.stream.impl.tx;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.stream.impl.AbstractCacheStream;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.stream.impl.DistributedDoubleCacheStream;
import org.infinispan.stream.impl.DistributedIntCacheStream;
import org.infinispan.stream.impl.DistributedLongCacheStream;

/**
 * Int variant of tx cache stream
 * @see TxDistributedCacheStream
 * @param <Original> original stream type
 * @param <K> the type of context entry key
 * @param <V> the type of context entry value
 */
public class TxDistributedIntCacheStream<Original, K, V> extends DistributedIntCacheStream<Original> {
   private final LocalTxInvocationContext ctx;
   private final ConsistentHash hash;
   private final Function<? super CacheEntry<K, V>, ? extends Original> toOriginalFunction;

   TxDistributedIntCacheStream(AbstractCacheStream stream, ConsistentHash hash, LocalTxInvocationContext ctx,
                               Function<? super CacheEntry<K, V>, ? extends Original> toOriginalFunction) {
      super(stream);
      this.ctx = ctx;
      this.hash = hash;
      this.toOriginalFunction = toOriginalFunction;
   }

   @Override
   protected Supplier<Stream<Original>> supplierForSegments(ConsistentHash ch, IntSet targetSegments,
           Set<Object> excludedKeys, boolean primaryOnly) {
      return () -> {
         Supplier<Stream<Original>> supplier = super.supplierForSegments(ch, targetSegments, excludedKeys, primaryOnly);
         Set<Original> set = new HashSet<>();
         ctx.forEachValue((key, entry) -> {
            if (!isPrimaryOwner(ch, key)) {
               Original apply = toOriginalFunction.apply((CacheEntry<K, V>) entry);
               set.add(apply);
            }
         });
         Stream<Original> suppliedStream = supplier.get();
         if (!set.isEmpty()) {
            return Stream.concat(set.stream(), suppliedStream);
         }
         return suppliedStream;
      };
   }

   @Override
   protected <R> DistributedCacheStream<Original, R> cacheStream() {
      return new TxDistributedCacheStream<>(this, localAddress, hash, ctx, toOriginalFunction);
   }

   @Override
   protected DistributedLongCacheStream<Original> longCacheStream() {
      return new TxDistributedLongCacheStream<>(this, localAddress, hash, ctx, toOriginalFunction);
   }

   @Override
   protected DistributedDoubleCacheStream<Original> doubleCacheStream() {
      return new TxDistributedDoubleCacheStream<>(this, localAddress, hash, ctx, toOriginalFunction);
   }
}
