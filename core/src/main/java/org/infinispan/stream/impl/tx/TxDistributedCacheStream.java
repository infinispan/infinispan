package org.infinispan.stream.impl.tx;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.CacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.AbstractCacheStream;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.stream.impl.DistributedDoubleCacheStream;
import org.infinispan.stream.impl.DistributedIntCacheStream;
import org.infinispan.stream.impl.DistributedLongCacheStream;

/**
 * A distributed cache stream that also utilizes transactional awareness.  Basically this adds functionality to
 * take items from the local tx context and add them to the local stream that is produced to enable our stream to
 * operate upon entries in the context that don't map to our segments that are normally ignored in a distributed
 * stream.
 * @param <Original> original stream type
 * @param <R> the type of stream
 * @param <K> the type of context entry key
 * @param <V> the type of context entry value
 */
public class TxDistributedCacheStream<Original, R, K, V> extends DistributedCacheStream<Original, R> {
   private final Address localAddress;
   private final LocalTxInvocationContext ctx;
   private final ConsistentHash hash;
   private final Function<? super CacheEntry<K, V>, ? extends Original> toOriginalFunction;

   public TxDistributedCacheStream(Address localAddress, boolean parallel, DistributionManager dm,
           Supplier<CacheStream<R>> supplier, TxClusterStreamManager<Original, K> csm, boolean includeLoader,
           int distributedBatchSize, Executor executor, ComponentRegistry registry, LocalTxInvocationContext ctx,
           Function<? super Original, ?> toKeyFunction, Function<? super CacheEntry<K, V>, ? extends Original> toOriginalFunction) {
      super(localAddress, parallel, dm, supplier, csm, includeLoader, distributedBatchSize, executor, registry, toKeyFunction);
      this.localAddress = localAddress;
      this.hash = dm.getWriteConsistentHash();
      this.ctx = ctx;
      this.toOriginalFunction = toOriginalFunction;
   }

   TxDistributedCacheStream(AbstractCacheStream other, Address localAddress, ConsistentHash hash,
           LocalTxInvocationContext ctx, Function<? super CacheEntry<K, V>, ? extends Original> toOriginalFunction) {
      super(other);
      this.localAddress = localAddress;
      this.hash = hash;
      this.ctx = ctx;
      this.toOriginalFunction = toOriginalFunction;
   }

   @Override
   protected Supplier<Stream<Original>> supplierForSegments(ConsistentHash ch, IntSet targetSegments,
           Set<Object> excludedKeys, boolean primaryOnly) {
      return () -> {
         Supplier<Stream<Original>> supplier = super.supplierForSegments(ch, targetSegments, excludedKeys, primaryOnly);
         // Now we have to add entries that aren't mapped to our local segments since we are excluding those
         // remotely via {@link TxClusterStreamManager} using the same hash
         List<Original> contextEntries = new ArrayList<>();
         ctx.forEachValue((key, entry) -> {
            if (!isPrimaryOwner(ch, key)) {
               contextEntries.add(toOriginalFunction.apply((CacheEntry<K, V>) entry));
            }
         });
         Stream<Original> suppliedStream = supplier.get();
         if (!contextEntries.isEmpty()) {
            return Stream.concat(contextEntries.stream(), suppliedStream);
         }
         return suppliedStream;
      };
   }

   @Override
   protected DistributedDoubleCacheStream<Original> doubleCacheStream() {
      return new TxDistributedDoubleCacheStream<Original, K, V>(this, localAddress, hash, ctx, toOriginalFunction);
   }

   @Override
   protected DistributedLongCacheStream<Original> longCacheStream() {
      return new TxDistributedLongCacheStream<Original, K, V>(this, localAddress, hash, ctx, toOriginalFunction);
   }

   @Override
   protected DistributedIntCacheStream<Original> intCacheStream() {
      return new TxDistributedIntCacheStream<Original, K, V>(this, hash, ctx, toOriginalFunction);
   }
}
