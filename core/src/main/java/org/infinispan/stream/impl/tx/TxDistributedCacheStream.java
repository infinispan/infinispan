package org.infinispan.stream.impl.tx;

import org.infinispan.CacheStream;
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

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A distributed cache stream that also utilizes transactional awareness.  Basically this adds functionality to
 * take items from the local tx context and add them to the local stream that is produced to enable our stream to
 * operate upon entries in the context that don't map to our segments that are normally ignored in a distributed
 * stream.
 * @param <R> the type of stream
 */
public class TxDistributedCacheStream<R> extends DistributedCacheStream<R> {
   private final Address localAddress;
   private final LocalTxInvocationContext ctx;
   private final ConsistentHash hash;

   public <K, V> TxDistributedCacheStream(Address localAddress, boolean parallel, DistributionManager dm,
           Supplier<CacheStream<CacheEntry<K, V>>> supplier, TxClusterStreamManager<?> csm, boolean includeLoader,
           int distributedBatchSize, Executor executor, ComponentRegistry registry, LocalTxInvocationContext ctx) {
      super(localAddress, parallel, dm, supplier, csm, includeLoader, distributedBatchSize, executor, registry);
      this.localAddress = localAddress;
      this.hash = dm.getConsistentHash();
      this.ctx = ctx;
   }

   public <K, V> TxDistributedCacheStream(Address localAddress, boolean parallel, DistributionManager dm,
           Supplier<CacheStream<CacheEntry<K, V>>> supplier, TxClusterStreamManager<?> csm, boolean includeLoader,
           int distributedBatchSize, Executor executor, ComponentRegistry registry,
           Function<? super CacheEntry<K, V>, R> function, LocalTxInvocationContext ctx) {
      super(localAddress, parallel, dm, supplier, csm, includeLoader, distributedBatchSize, executor, registry, function);
      this.localAddress = localAddress;
      this.hash = dm.getConsistentHash();
      this.ctx = ctx;
   }

   TxDistributedCacheStream(AbstractCacheStream other, Address localAddress, ConsistentHash hash,
           LocalTxInvocationContext ctx) {
      super(other);
      this.localAddress = localAddress;
      this.hash = hash;
      this.ctx = ctx;
   }

   @Override
   protected Supplier<Stream<CacheEntry>> supplierForSegments(ConsistentHash ch, Set<Integer> targetSegments,
           Set<Object> excludedKeys, boolean primaryOnly) {
      return () -> {
         Supplier<Stream<CacheEntry>> supplier = super.supplierForSegments(ch, targetSegments, excludedKeys, primaryOnly);
         // Now we have to add entries that aren't mapped to our local segments since we are excluding those
         // remotely via {@link TxClusterStreamManager} using the same hash
         Set<CacheEntry> set = ctx.getLookedUpEntries().values().stream().filter(
                 e -> !localAddress.equals(ch.locatePrimaryOwner(e.getKey()))).collect(Collectors.toSet());
         Stream<CacheEntry> suppliedStream = supplier.get();
         if (!set.isEmpty()) {
            return Stream.concat(set.stream(), suppliedStream);
         }
         return suppliedStream;
      };
   }

   @Override
   protected DistributedDoubleCacheStream doubleCacheStream() {
      return new TxDistributedDoubleCacheStream(this, localAddress, hash, ctx);
   }

   @Override
   protected DistributedLongCacheStream longCacheStream() {
      return new TxDistributedLongCacheStream(this, localAddress, hash, ctx);
   }

   @Override
   protected DistributedIntCacheStream intCacheStream() {
      return new TxDistributedIntCacheStream(this, localAddress, hash, ctx);
   }
}
