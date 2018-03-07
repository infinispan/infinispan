package org.infinispan.stream.impl.tx;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.infinispan.commons.util.IntSet;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.TerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.AbstractDelegatingMap;

/**
 * This is a delegating cluster stream manager that sends all calls to the underlying cluster stream manager.  However
 * in the case of performing an operation it adds all entries that are in the provided tx context to the map of
 * keys to exclude so those values are not processed in the remote nodes.
 * @param <K> the key type
 */
public class TxClusterStreamManager<Original, K> implements ClusterStreamManager<Original, K> {
   private final ClusterStreamManager<Original, K> manager;
   private final LocalTxInvocationContext ctx;
   private final int maxSegments;
   private final ToIntFunction<Object> intFunction;

   public TxClusterStreamManager(ClusterStreamManager<Original, K> manager, LocalTxInvocationContext ctx, int maxSegments,
         ToIntFunction<Object> intFunction) {
      this.manager = manager;
      this.ctx = ctx;
      this.maxSegments = maxSegments;
      this.intFunction = intFunction;
   }

   @Override
   public <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, TerminalOperation<Original, R> operation, ResultsCallback<R> callback,
         Predicate<? super R> earlyTerminatePredicate) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, intFunction);
      return manager.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, entryStream, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R> Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
         ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
         boolean includeLoader, boolean entryStream, TerminalOperation<Original, R> operation,
         ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, intFunction);
      return manager.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, entryStream, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, KeyTrackingTerminalOperation<Original, K, R> operation,
         ResultsCallback<Collection<R>> callback) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, intFunction);
      return manager.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, entryStream, operation, callback);
   }

   @Override
   public Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, boolean entryStream, KeyTrackingTerminalOperation<Original, K, ?> operation,
         ResultsCallback<Collection<K>> callback) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, intFunction);
      return manager.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, entryStream, operation, callback);
   }

   @Override
   public boolean isComplete(Object id) {
      return manager.isComplete(id);
   }

   @Override
   public boolean awaitCompletion(Object id, long time, TimeUnit unit) throws InterruptedException {
      return manager.awaitCompletion(id, time, unit);
   }

   @Override
   public void forgetOperation(Object id) {
      manager.forgetOperation(id);
   }

   @Override
   public <R1> boolean receiveResponse(Object id, Address origin, boolean complete, Set<Integer> segments, R1 response) {
      return manager.receiveResponse(id, origin, complete, segments, response);
   }

   @Override
   public <E> RemoteIteratorPublisher<E> remoteIterationPublisher(boolean parallelStream,
         Supplier<Map.Entry<Address, IntSet>> segments, Set<K> keysToInclude, IntFunction<Set<K>> keysToExclude,
         boolean includeLoader, boolean entryStream, Iterable<IntermediateOperation> intermediateOperations) {

      if (ctx.getLookedUpEntries().isEmpty()) {
         return manager.remoteIterationPublisher(parallelStream, segments, keysToInclude, keysToExclude, includeLoader,
               entryStream, intermediateOperations);
      } else {
         Set<K>[] contextSet = generateContextSet(ctx);
         if (keysToExclude == null) {
            return manager.remoteIterationPublisher(parallelStream, segments, keysToInclude, i -> contextSet[i],
                  includeLoader, entryStream, intermediateOperations);
         } else {
            return manager.remoteIterationPublisher(parallelStream, segments, keysToInclude, i -> {
               Set<K> set = contextSet[i];
               if (set != null) {
                  set.addAll(keysToExclude.apply(i));
                  return set;
               } else {
                  return keysToExclude.apply(i);
               }
            }, includeLoader, entryStream, intermediateOperations);
         }
      }
   }

   Set<K>[] generateContextSet(LocalTxInvocationContext ctx) {
      Set<K>[] set = new Set[maxSegments];
      ctx.getLookedUpEntries().forEach((k, v) -> {
         int segment = intFunction.applyAsInt(k);
         Set<K> innerSet = set[segment];
         if (innerSet == null) {
            innerSet = new HashSet<>();
            set[segment] = innerSet;
         }
         innerSet.add((K) k);
      });
      return set;
   }

   private static class TxExcludedKeys<K> extends AbstractDelegatingMap<Integer, Set<K>> {
      private final Map<Integer, Set<K>> map;
      private final Map<Integer, Set<K>> ctxMap;

      private TxExcludedKeys(Map<Integer, Set<K>> map, LocalTxInvocationContext ctx, ToIntFunction<Object> intFunction) {
         this.map = map;
         this.ctxMap = contextToMap(ctx, intFunction);
      }

      Map<Integer, Set<K>> contextToMap(LocalTxInvocationContext ctx, ToIntFunction<Object> intFunction) {
         Map<Integer, Set<K>> contextMap = new HashMap<>();
         ctx.getLookedUpEntries().forEach((k, v) -> {
            Integer segment = intFunction.applyAsInt(k);
            Set<K> innerSet = contextMap.get(segment);
            if (innerSet == null) {
               innerSet = new HashSet<K>();
               contextMap.put(segment, innerSet);
            }
            innerSet.add((K) k);
         });
         return contextMap;
      }

      @Override
      protected Map<Integer, Set<K>> delegate() {
         return map;
      }

      @Override
      public Set<K> get(Object key) {
         if (!(key instanceof Integer)) {
            return null;
         }
         Set<K> ctxSet = ctxMap.get(key);
         Set<K> excludedSet = super.get(key);
         if (ctxSet != null) {
            if (excludedSet != null) {
               ctxSet.addAll(excludedSet);
            }
            return ctxSet;
         }
         return excludedSet;
      }

      @Override
      public boolean isEmpty() {
         return ctxMap.isEmpty() && super.isEmpty();
      }
   }
}
