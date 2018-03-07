package org.infinispan.stream.impl.tx;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.AbstractCacheStream;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.stream.impl.DistributedDoubleCacheStream;
import org.infinispan.stream.impl.DistributedIntCacheStream;
import org.infinispan.stream.impl.DistributedLongCacheStream;

/**
 * Long variant of tx cache stream
 * @see TxDistributedCacheStream
 * @param <Original> original stream type
 * @param <K> the type of context entry key
 * @param <V> the type of context entry value
 */
public class TxDistributedLongCacheStream<Original, K, V> extends DistributedLongCacheStream<Original> {
   private final Address localAddress;
   private final LocalTxInvocationContext ctx;
   private final ConsistentHash hash;
   private final Function<? super CacheEntry<K, V>, ? extends Original> toOriginalFunction;

   TxDistributedLongCacheStream(AbstractCacheStream stream, Address localAddress, ConsistentHash hash,
         LocalTxInvocationContext ctx, Function<? super CacheEntry<K, V>, ? extends Original> toOriginalFunction) {
      super(stream);
      this.localAddress = localAddress;
      this.ctx = ctx;
      this.hash = hash;
      this.toOriginalFunction = toOriginalFunction;
   }

   @Override
   protected Supplier<Stream<Original>> supplierForSegments(ConsistentHash ch, IntSet targetSegments,
           Set<Object> excludedKeys, boolean primaryOnly) {
      return () -> {
         Supplier<Stream<Original>> supplier = super.supplierForSegments(ch, targetSegments, excludedKeys, primaryOnly);
         Set<Original> set = ctx.getLookedUpEntries().values().stream()
                                  .filter(e -> !isPrimaryOwner(ch, e))
                                  .map(e -> toOriginalFunction.apply((CacheEntry<K, V>) e))
                                  .collect(Collectors.toSet());
         Stream<Original> suppliedStream = supplier.get();
         if (!set.isEmpty()) {
            return Stream.concat(set.stream(), suppliedStream);
         }
         return suppliedStream;
      };
   }

   @Override
   protected <R> DistributedCacheStream<Original, R> cacheStream() {
      return new TxDistributedCacheStream<Original, R, K, V>(this, localAddress, hash, ctx, toOriginalFunction);
   }

   @Override
   protected DistributedIntCacheStream<Original> intCacheStream() {
      return new TxDistributedIntCacheStream<Original, K, V>(this, localAddress, hash, ctx, toOriginalFunction);
   }

   @Override
   protected DistributedDoubleCacheStream<Original> doubleCacheStream() {
      return new TxDistributedDoubleCacheStream<Original, K, V>(this, localAddress, hash, ctx, toOriginalFunction);
   }
}
