package org.infinispan.expiration.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.expiration.TouchMode;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

/**
 * Allows for cluster based expirations to occur.  This provides guarantees that when an entry is expired that it will
 * expire that entry across the entire cluster at once.  This requires obtaining the lock for said entry before
 * expiration is performed.  Since expiration can occur without holding onto the lock it is possible for an expiration
 * to occur immediately after a value has been updated.  This can cause a premature expiration to occur.  Attempts
 * are made to prevent this by using the expired entry's value and lifespan to limit this expiration so it only happens
 * in a smaller amount of cases.
 * <p>
 * Cache stores however do not supply the value or metadata information which means if an entry is purged from the cache
 * store that it will forcibly remove the value even if a concurrent write updated it just before.  This will be
 * addressed by future SPI changes to the cache store.
 * @param <K>
 * @param <V>
 */
@Scope(Scopes.NAMED_CACHE)
@ThreadSafe
public class ClusterExpirationManager<K, V> extends ExpirationManagerImpl<K, V> {
   private static final Log log = LogFactory.getLog(ClusterExpirationManager.class);

   /**
    * Defines the maximum number of allowed concurrent expirations. Any expirations over this must wait until
    * another has completed before processing
    */
   private static final int MAX_CONCURRENT_EXPIRATIONS = 100;

   @Inject protected RpcManager rpcManager;
   @Inject protected DistributionManager distributionManager;

   private Address localAddress;
   private long timeout;
   private TouchMode touchMode;

   @Override
   public void start() {
      super.start();
      this.localAddress = cache.getCacheManager().getAddress();
      this.timeout = configuration.clustering().remoteTimeout();
      if (configuration.clustering().cacheMode().isSynchronous()) {
         touchMode = configuration.expiration().touch();
      } else {
         touchMode = TouchMode.ASYNC;
      }
      configuration.clustering()
                   .attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .addListener((a, ignored) -> {
                      timeout = a.get();
                   });
   }

   @Override
   public void processExpiration() {

      if (!Thread.currentThread().isInterrupted()) {
         LocalizedCacheTopology topology;
         // Purge all contents until we know we did so with a stable topology
         do {
            topology = distributionManager.getCacheTopology();
         } while (purgeInMemoryContents(topology));
      }

      if (!Thread.currentThread().isInterrupted()) {
         CompletionStages.join(persistenceManager.purgeExpired());
      }
   }

   /**
    * Purges in memory contents removing any expired entries.
    * @return true if there was a topology change
    */
   private boolean purgeInMemoryContents(LocalizedCacheTopology topology) {
      long start = 0;
      int removedEntries = 0;
      AtomicInteger errors = new AtomicInteger();
      try {
         if (log.isTraceEnabled()) {
            log.tracef("Purging data container on cache %s for topology %d", cacheName, topology.getTopologyId());
            start = timeService.time();
         }
         // We limit how many non blocking expiration removals performed concurrently
         // The addition to the queue shouldn't ever block but rather pollForCompletion when we are waiting for
         // prior tasks to complete
         BlockingQueue<CompletableFuture<?>> expirationPermits = new ArrayBlockingQueue<>(MAX_CONCURRENT_EXPIRATIONS);
         long currentTimeMillis = timeService.wallClockTime();

         IntSet segments;
         if (topology.getReadConsistentHash().getMembers().contains(localAddress)) {
            segments = IntSets.from(topology.getReadConsistentHash().getPrimarySegmentsForOwner(localAddress));
         } else {
            segments = IntSets.immutableEmptySet();
         }

         for (Iterator<InternalCacheEntry<K, V>> purgeCandidates = dataContainer.running().iteratorIncludingExpired(segments);
              purgeCandidates.hasNext();) {
            InternalCacheEntry<K, V> ice = purgeCandidates.next();
            if (ice.canExpire()) {
               // Have to synchronize on the entry to make sure we see the value and metadata at the same time
               boolean expiredMortal;
               boolean expiredTransient;
               V value;
               long lifespan;
               long maxIdle;
               synchronized (ice) {
                  value = ice.getValue();
                  lifespan = ice.getLifespan();
                  maxIdle = ice.getMaxIdle();
                  expiredMortal = ExpiryHelper.isExpiredMortal(lifespan, ice.getCreated(), currentTimeMillis);
                  expiredTransient = ExpiryHelper.isExpiredTransient(maxIdle, ice.getLastUsed(), currentTimeMillis);
               }
               if (expiredMortal || expiredTransient) {
                  // Any expirations over the max must check for another to finish before it can proceed
                  if (++removedEntries > MAX_CONCURRENT_EXPIRATIONS && !pollForCompletion(expirationPermits, start, removedEntries, errors)) {
                     return false;
                  }
                  CompletableFuture<?> stage;
                  // If the entry is expired both wrt lifespan and wrt maxIdle, we perform lifespan expiration as it is cheaper
                  if (expiredMortal) {
                     stage = handleLifespanExpireEntry(ice.getKey(), value, lifespan, false);
                  } else {
                     stage = handleMaxIdleExpireEntry(ice, false, currentTimeMillis);
                  }
                  stage.whenComplete((obj, t) -> addStageToPermits(expirationPermits, stage));
               }
            }
            // Short circuit if topology has changed
            if (distributionManager.getCacheTopology() != topology) {
               printResults("Purging data container on cache %s stopped due to topology change. Total time was: %s and removed %d entries with %d errors", start, removedEntries, errors);
               return true;
            }
         }
         // We wait for any pending expiration to complete before returning
         int expirationsLeft = Math.min(removedEntries, MAX_CONCURRENT_EXPIRATIONS);
         for (int i = 0; i < expirationsLeft; ++i) {
            if (!pollForCompletion(expirationPermits, start, removedEntries, errors)) {
               return false;
            }
         }
         printResults("Purging data container on cache %s completed in %s and removed %d entries with %d errors", start, removedEntries, errors);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         printResults("Purging data container on cache %s was interrupted. Total time was: %s and removed %d entries with %d errors", start, removedEntries, errors);
      } catch (Throwable t) {
         log.exceptionPurgingDataContainer(t);
      }
      return false;
   }

   /**
    * This is a separate method to document the fact that this is invoked in a separate thread and also for code
    * augmentation to find this method if needed
    * <p>
    * This method should never block as the queue should always have room by design
    * @param expirationPermits the permits blocking queue to add to
    * @param future the future to add to it.
    */
   void addStageToPermits(BlockingQueue<CompletableFuture<?>> expirationPermits, CompletableFuture<?> future) {
      boolean inserted = expirationPermits.offer(future);
      assert inserted;
   }

   private void printResults(String message, long start, int removedEntries, AtomicInteger errors) {
      if (log.isTraceEnabled()) {
         log.tracef(message, cacheName, Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)), removedEntries, errors.get());
      }
   }

   /**
    * Polls for an expiration to complete, returning whether an expiration completed in the time allotted. If an
    * expiration has completed it will also verify it is had errored and update the count and log the message.
    * @param queue the queue to poll from to find a completed expiration
    * @param start the start time of the processing
    * @param removedEntries the current count of expired entries
    * @param errors value to increment when an error occurs
    * @return true if an expiration completed
    * @throws InterruptedException if the polling is interrupted
    */
   private boolean pollForCompletion(BlockingQueue<CompletableFuture<?>> queue, long start, int removedEntries,
         AtomicInteger errors) throws InterruptedException, TimeoutException {
      CompletableFuture<?> future;
      if ((future = queue.poll(timeout * 3, TimeUnit.MILLISECONDS)) != null) {
         try {
            // This shouldn't block
            future.get(100, TimeUnit.MILLISECONDS);
         } catch (ExecutionException e) {
            Throwable ce = e.getCause();
            while (ce instanceof RemoteException) {
               ce = ce.getCause();
            }
            if (ce instanceof org.infinispan.util.concurrent.TimeoutException) {
               // Ignoring a TimeoutException as it could just be the entry was being updated concurrently
               log.tracef(e, "Encountered timeout exception, assuming it was due to a concurrent write. Entry will" +
                     " be attempted to be removed on the next purge if still expired.");
            } else {
               errors.incrementAndGet();
               log.exceptionPurgingDataContainer(e.getCause());
            }
         }
         return true;
      }
      printResults("Purging data container on cache %s stopped due to waiting for prior removal (could be a bug or misconfiguration). Total time was: %s and removed %d entries", start, removedEntries, errors);
      return false;
   }

   // Skip locking is required in case if the entry was found expired due to a write, which would already
   // have the lock and remove lifespan expired is fired in a different thread with a different context. Otherwise the
   // expiration may occur in the wrong order and events may also be raised in the incorrect order. We assume the caller
   // holds the lock until this CompletableFuture completes. Without lock skipping this would deadlock.
   CompletableFuture<Boolean> handleLifespanExpireEntry(K key, V value, long lifespan, boolean isWrite) {
      return handleEitherExpiration(key, value, false, lifespan, isWrite);
   }

   CompletableFuture<Boolean> removeLifespan(AdvancedCache<K, V> cacheToUse, K key, V value, long lifespan) {
      return cacheToUse.removeLifespanExpired(key, value, lifespan);
   }

   CompletableFuture<Boolean> handleMaxIdleExpireEntry(InternalCacheEntry<K, V> entry, boolean isWrite, long currentTime) {
      return handleEitherExpiration(entry.getKey(), entry.getValue(), true, entry.getMaxIdle(), isWrite)
              .thenCompose(expired -> {
                 if (!expired) {
                    if (log.isTraceEnabled()) {
                       log.tracef("Entry was not actually expired via max idle - touching on all nodes");
                    }
                    // TODO: what do we do if another node couldn't touch the value?
                    return checkExpiredMaxIdle(entry, keyPartitioner.getSegment(entry.getKey()), currentTime)
                          .thenApply(ignore -> Boolean.FALSE);
                 }
                 return CompletableFutures.completedTrue();
              });
   }

   CompletableFuture<Boolean> removeMaxIdle(AdvancedCache<K, V> cacheToUse, K key, V value) {
      return cacheToUse.removeMaxIdleExpired(key, value);
   }

   private CompletableFuture<Boolean> handleEitherExpiration(K key, V value, boolean maxIdle, long time, boolean isWrite) {
      CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
      CompletableFuture<Boolean> previousFuture = expiring.putIfAbsent(key, completableFuture);
      if (previousFuture == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Submitting expiration removal for key: %s which is maxIdle: %s of: %s", toStr(key), maxIdle, time);
         }
         try {
            AdvancedCache<K, V> cacheToUse = cacheToUse(isWrite);
            CompletableFuture<Boolean> future;
            if (maxIdle) {
               future = removeMaxIdle(cacheToUse, key, value);
            } else {
               future = removeLifespan(cacheToUse, key, value, time);
            }
            return future.whenComplete((b, t) -> {
               // We have to remove the entry from the map before setting the exception status - otherwise retry for
               // a write at the same time could get stuck in a recursive loop
               expiring.remove(key);
               if (t != null) {
                  completableFuture.completeExceptionally(t);
               } else {
                  completableFuture.complete(b);
               }
            });
         } catch (Throwable t) {
            // We have to remove the entry from the map before setting the exception status - otherwise retry for
            // a write at the same time could get stuck in a recursive loop
            expiring.remove(key);
            completableFuture.completeExceptionally(t);
            throw t;
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("There is a pending expiration removal for key %s, waiting until it completes.", key);
      }
      // This means there was another thread that found it had expired via max idle or we have optimistic tx
      return previousFuture;
   }

   Throwable getMostNestedSuppressedThrowable(Throwable t) {
      Throwable nested = getNestedThrowable(t);
      Throwable[] suppressedNested = nested.getSuppressed();
      if (suppressedNested.length > 0) {
         nested = getNestedThrowable(suppressedNested[0]);
      }
      return nested;
   }

   Throwable getNestedThrowable(Throwable t) {
      Throwable cause;
      while ((cause = t.getCause()) != null) {
         t = cause;
      }
      return t;
   }

   AdvancedCache<K, V> cacheToUse(boolean isWrite) {
      return isWrite ? cache.withFlags(Flag.SKIP_LOCKING) : cache.withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime,
         boolean isWrite) {
      // We need to synchronize on the entry since {@link InternalCacheEntry} locks the entry when doing an update
      // so we can see both the new value and the metadata
      boolean expiredMortal;
      V value;
      long lifespan;
      synchronized (entry) {
         value = entry.getValue();
         lifespan = entry.getLifespan();
         expiredMortal = ExpiryHelper.isExpiredMortal(lifespan, entry.getCreated(), currentTime);
      }
      CompletableFuture<Boolean> future;
      if (expiredMortal) {
         future = handleLifespanExpireEntry(entry.getKey(), value, lifespan, isWrite);
         // We don't want to block the user while the remove expired is happening for lifespan on a read
         if (!waitOnLifespanExpiration(isWrite)) {
            return CompletableFutures.completedTrue();
         }
      } else {
         // This means it expired transiently - this will block user until we confirm the entry is okay
         future = handleMaxIdleExpireEntry(entry, isWrite, currentTime);
      }

      return future.handle((expired, t) -> {
         if (t != null) {
            // With optimistic transaction the TimeoutException is nested as the following:
            // CompletionException
            // -> caused by RollbackException
            //    -> suppressing XAException
            //       -> caused by RemoteException
            //         -> caused by TimeoutException

            // In Pessimistic or Non tx it is just a CompletionException wrapping a TimeoutException
            Throwable cause = getMostNestedSuppressedThrowable(t);
            // Note this exception is from a prior registered stage, not our own. If the prior one got a TimeoutException
            // we try to reregister our write to do a remove expired call. This can happen if a read
            // spawned remove expired times out as it has a 0 lock acquisition timeout. We don't want to propagate
            // the TimeoutException to the caller of the write command as it is unrelated.
            // Note that the remove expired command is never "retried" itself.
            if (cause instanceof org.infinispan.util.concurrent.TimeoutException) {
               if (log.isTraceEnabled()) {
                  log.tracef("Encountered timeout exception in remove expired invocation - need to retry!");
               }
               // Have to try the command ourselves as the prior one timed out (this could be valid if a read based
               // remove expired couldn't acquire the try lock
               return entryExpiredInMemory(entry, currentTime, isWrite);
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef(t, "Encountered exception in remove expired invocation - propagating!");
               }
               return CompletableFutures.<Boolean>completedExceptionFuture(cause);
            }
         } else {
            return expired == Boolean.TRUE ? CompletableFutures.completedTrue() : CompletableFutures.completedFalse();
         }
      }).thenCompose(Function.identity());
   }

   // Designed to be overridden as needed
   boolean waitOnLifespanExpiration(boolean isWrite) {
      // We always have to wait for the prior expiration to complete if we are holding the lock. Otherwise we can have
      // multiple invocations running at the same time.
      return isWrite;
   }

   @Override
   public boolean entryExpiredInMemoryFromIteration(InternalCacheEntry<K, V> entry, long currentTime) {
      // We need to synchronize on the entry since {@link InternalCacheEntry} locks the entry when doing an update
      // so we can see both the new value and the metadata
      boolean expiredMortal;
      synchronized (entry) {
         expiredMortal = ExpiryHelper.isExpiredMortal(entry.getLifespan(), entry.getCreated(), currentTime);
      }
      // Note we don't check for transient expiration (maxIdle). We always ignore those as it would require remote
      // overhead to confirm. Instead we only return if the entry expired mortally (lifespan) as we always expire
      // entries that are found to be in this state.
      return expiredMortal;
   }

   @Override
   public CompletionStage<Void> handleInStoreExpirationInternal(K key) {
      return handleInStoreExpirationInternal(key, null);
   }

   @Override
   public CompletionStage<Void> handleInStoreExpirationInternal(MarshallableEntry<K, V> marshalledEntry) {
      return handleInStoreExpirationInternal(marshalledEntry.getKey(), marshalledEntry);
   }

   private CompletionStage<Void> handleInStoreExpirationInternal(K key, MarshallableEntry<K, V> marshallableEntry) {
      CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
      CompletableFuture<Boolean> previousFuture = expiring.putIfAbsent(key, completableFuture);
      if (previousFuture == null) {
         AdvancedCache<K, V> cacheToUse = cache.withFlags(Flag.SKIP_SHARED_CACHE_STORE, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
         CompletionStage<Boolean> resultStage;
         if (marshallableEntry != null) {
            Metadata metadata = marshallableEntry.getMetadata();
            resultStage = cacheToUse.removeLifespanExpired(key, marshallableEntry.getValue(),
                  metadata == null || metadata.lifespan() == -1 ? null : metadata.lifespan());
         } else {
            // Unfortunately stores don't pull the entry so we can't tell exactly why it expired and thus we have to remove
            // the entire value.  Unfortunately this could cause a concurrent write to be undone
            resultStage = cacheToUse.removeLifespanExpired(key, null, null);
         }
         return CompletionStages.ignoreValue(resultStage.whenComplete((b, t) -> {
            expiring.remove(key);
            if (t != null) {
               completableFuture.completeExceptionally(t);
            } else {
               completableFuture.complete(b);
            }
         }));
      }
      return CompletionStages.ignoreValue(previousFuture);
   }

   @Override
   protected CompletionStage<Boolean> checkExpiredMaxIdle(InternalCacheEntry ice, int segment, long currentTime) {
      return attemptTouchAndReturnIfExpired(ice, segment, currentTime)
            .handle((expired, t) -> {
               if (t != null) {
                  Throwable innerT = CompletableFutures.extractException(t);
                  if (innerT instanceof OutdatedTopologyException) {
                     if (log.isTraceEnabled()) {
                        log.tracef("Touch received OutdatedTopologyException, retrying");
                     }
                     return attemptTouchAndReturnIfExpired(ice, segment, currentTime);
                  }
                  return CompletableFutures.<Boolean>completedExceptionFuture(t);
               } else {
                  return CompletableFuture.completedFuture(expired);
               }
            })
            .thenCompose(Function.identity());
   }

   private CompletionStage<Boolean> attemptTouchAndReturnIfExpired(InternalCacheEntry ice, int segment, long currentTime) {
      return cache.touch(ice.getKey(), segment, true)
            .thenApply(touched -> {
               if (touched) {
                  // The ICE can be a copy in cases such as off-heap - we need to update its time that is reported to the user
                  ice.touch(currentTime);
               }
               return !touched;
            });
   }
}
