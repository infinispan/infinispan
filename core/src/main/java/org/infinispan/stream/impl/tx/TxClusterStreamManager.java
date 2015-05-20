package org.infinispan.stream.impl.tx;

import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.TerminalOperation;
import org.infinispan.util.AbstractDelegatingMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * This is a delegating cluster stream manager that sends all calls to the underlying cluster stream manager.  However
 * in the case of performing an operation it adds all entries that are in the provided tx context to the map of
 * keys to exclude so those values are not processed in the remote nodes.
 * @param <K> the key type
 */
public class TxClusterStreamManager<K> implements ClusterStreamManager<K> {
   private final ClusterStreamManager<K> manager;
   private final LocalTxInvocationContext ctx;
   private final ConsistentHash hash;

   public TxClusterStreamManager(ClusterStreamManager<K> manager, LocalTxInvocationContext ctx, ConsistentHash hash) {
      this.manager = manager;
      this.ctx = ctx;
      this.hash = hash;
   }

   @Override
   public <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           TerminalOperation<R> operation, ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, hash);
      return manager.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, TerminalOperation<R> operation, ResultsCallback<R> callback,
           Predicate<? super R> earlyTerminatePredicate) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, hash);
      return manager.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, R, ?> operation, ResultsCallback<Collection<R>> callback) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, hash);
      return manager.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, operation, callback);
   }

   @Override
   public <R2> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, KeyTrackingTerminalOperation<K, ?, R2> operation, ResultsCallback<Map<K, R2>> callback) {
      TxExcludedKeys<K> txExcludedKeys = new TxExcludedKeys<>(keysToExclude, ctx, hash);
      return manager.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              txExcludedKeys, includeLoader, operation, callback);
   }

   @Override
   public boolean isComplete(UUID id) {
      return manager.isComplete(id);
   }

   @Override
   public boolean awaitCompletion(UUID id, long time, TimeUnit unit) throws InterruptedException {
      return manager.awaitCompletion(id, time, unit);
   }

   @Override
   public void forgetOperation(UUID id) {
      manager.forgetOperation(id);
   }

   @Override
   public <R1> boolean receiveResponse(UUID id, Address origin, boolean complete, Set<Integer> segments, R1 response) {
      return manager.receiveResponse(id, origin, complete, segments, response);
   }

   private static class TxExcludedKeys<K> extends AbstractDelegatingMap<Integer, Set<K>> {
      private final Map<Integer, Set<K>> map;
      private final Map<Integer, Set<K>> ctxMap;

      private TxExcludedKeys(Map<Integer, Set<K>> map, LocalTxInvocationContext ctx, ConsistentHash hash) {
         this.map = map;
         this.ctxMap = contextToMap(ctx, hash);
      }

      Map<Integer, Set<K>> contextToMap(LocalTxInvocationContext ctx, ConsistentHash hash) {
         Map<Integer, Set<K>> contextMap = new HashMap<>();
         ctx.getLookedUpEntries().forEach((k, v) -> {
            Integer segment = hash.getSegment(k);
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
