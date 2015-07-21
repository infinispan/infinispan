package org.infinispan.stream.impl;

import org.infinispan.Cache;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.AvailabilityMode;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Cluster stream manager that also pays attention to partition status and properly closes iterators and throws
 * exceptions when the availability mode changes.
 */
public class PartitionAwareClusterStreamManager<K> extends ClusterStreamManagerImpl<K> {
   protected final PartitionListener listener;
   protected Cache<?, ?> cache;

   public PartitionAwareClusterStreamManager() {
      this.listener = new PartitionListener();
   }

   @Listener
   protected class PartitionListener {
      protected volatile AvailabilityMode currentMode = AvailabilityMode.AVAILABLE;

      @PartitionStatusChanged
      public void onPartitionChange(PartitionStatusChangedEvent<K, ?> event) {
         if (!event.isPre()) {
            currentMode = event.getAvailabilityMode();
            if (currentMode != AvailabilityMode.AVAILABLE) {
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
   public boolean awaitCompletion(UUID id, long time, TimeUnit unit) throws InterruptedException {
      checkPartitionStatus();
      return super.awaitCompletion(id, time, unit);
   }

   @Override
   public <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           TerminalOperation<R> operation, ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate) {
      checkPartitionStatus();
      return super.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, R, ?> operation, ResultsCallback<Collection<R>> callback) {
      checkPartitionStatus();
      return super.remoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback);
   }

   @Override
   public <R> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, TerminalOperation<R> operation, ResultsCallback<R> callback,
           Predicate<? super R> earlyTerminatePredicate) {
      checkPartitionStatus();
      return super.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback, earlyTerminatePredicate);
   }

   @Override
   public <R2> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, KeyTrackingTerminalOperation<K, ?, R2> operation,
           ResultsCallback<Map<K, R2>> callback) {
      checkPartitionStatus();
      return super.remoteStreamOperationRehashAware(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback);
   }

   private void checkPartitionStatus() {
      if (listener.currentMode != AvailabilityMode.AVAILABLE) {
         throw log.partitionDegraded();
      }
   }
}
