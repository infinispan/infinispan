package org.infinispan.stream.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Cluster stream manager that also pays attention to partition status and properly closes iterators and throws
 * exceptions when the availability mode changes.
 */
public class PartitionAwareClusterStreamManager<K> extends ClusterStreamManagerImpl<K> {
   protected final PartitionListener listener;
   protected Cache<?, ?> cache;
   private PartitionHandling partitionHandling;

   public PartitionAwareClusterStreamManager() {
      this.listener = new PartitionListener();
   }

   @Inject
   public void init(Configuration configuration) {
      this.partitionHandling = configuration.clustering().partitionHandling().whenSplit();
   }

   @Listener
   private class PartitionListener {
      volatile AvailabilityMode currentMode = AvailabilityMode.AVAILABLE;

      @PartitionStatusChanged
      public void onPartitionChange(PartitionStatusChangedEvent<K, ?> event) {
         if (!event.isPre()) {
            currentMode = event.getAvailabilityMode();
            if (isPartitionDegraded()) {
               // We just mark the iterator - relying on the fact that callers must call forget properly
               currentlyRunning.values().forEach(t ->
                       markTrackerWithException(t, null, new AvailabilityException(), null));
            }
         }
      }
   }

   @Inject
   public void inject(Cache<?, ?> cache) {
      this.cache = cache;
   }

   @Start
   public void start() {
      super.start();
      cache.addListener(listener);
   }

   @Override
   public boolean awaitCompletion(Object id, long time, TimeUnit unit) throws InterruptedException {
      checkPartitionStatus();
      return super.awaitCompletion(id, time, unit);
   }

   @Override
   public <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           TerminalOperation<R> operation, ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate) {
      checkPartitionStatus();
      return super.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, R, ?> operation, ResultsCallback<Collection<R>> callback) {
      checkPartitionStatus();
      return super.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback);
   }

   @Override
   public <R> Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, TerminalOperation<R> operation, ResultsCallback<R> callback,
           Predicate<? super R> earlyTerminatePredicate) {
      checkPartitionStatus();
      return super.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R2> Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, KeyTrackingTerminalOperation<K, ?, R2> operation,
           ResultsCallback<Map<K, R2>> callback) {
      checkPartitionStatus();
      return super.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback);
   }

   @Override
   public <E> RemoteIteratorPublisher<E> remoteIterationPublisher(boolean parallelStream,
         Supplier<Map.Entry<Address, IntSet>> targets, Set<K> keysToInclude, IntFunction<Set<K>> keysToExclude,
         boolean includeLoader, Iterable<IntermediateOperation> intermediateOperations) {
      checkPartitionStatus();
      return super.remoteIterationPublisher(parallelStream, targets, keysToInclude, keysToExclude, includeLoader, intermediateOperations);
   }

   private void checkPartitionStatus() {
      if (isPartitionDegraded()) {
         throw log.partitionDegraded();
      }
   }

   private boolean isPartitionDegraded() {
      return listener.currentMode != AvailabilityMode.AVAILABLE && partitionHandling == PartitionHandling.DENY_READ_WRITES;
   }
}
