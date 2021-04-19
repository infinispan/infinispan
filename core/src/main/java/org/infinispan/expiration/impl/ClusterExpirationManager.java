package org.infinispan.expiration.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.expiration.TouchMode;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidResponseCollector;
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
@ThreadSafe
public class ClusterExpirationManager<K, V> extends ExpirationManagerImpl<K, V> {
   private static final Log log = LogFactory.getLog(ClusterExpirationManager.class);
   private static final boolean trace = log.isTraceEnabled();

   /**
    * Defines the maximum number of allowed concurrent expirations. Any expirations over this must wait until
    * another has completed before processing
    */
   private static final int MAX_CONCURRENT_EXPIRATIONS = 100;

   @Inject protected ComponentRef<AdvancedCache<K, V>> cacheRef;
   @Inject protected RpcManager rpcManager;
   @Inject protected DistributionManager distributionManager;
   @Inject @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
   protected ExecutorService asyncExecutor;

   protected AdvancedCache<K, V> cache;
   private Address localAddress;
   private long timeout;
   private String cacheName;
   private boolean transactional;
   private TouchMode touchMode;

   @Override
   public void start() {
      super.start();
      // Data container entries are retrieved directly, so we don't need to worry about an encodings
      this.cache = AbstractDelegatingCache.unwrapCache(cacheRef.wired()).getAdvancedCache();
      this.cacheName = cache.getName();
      this.localAddress = cache.getCacheManager().getAddress();
      this.timeout = configuration.clustering().remoteTimeout();

      transactional = configuration.transaction().transactionMode().isTransactional();
      if (configuration.clustering().cacheMode().isSynchronous()) {
         touchMode = configuration.expiration().touch();
      } else {
         touchMode = TouchMode.ASYNC;
      }
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
         persistenceManager.purgeExpired();
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
         if (trace) {
            log.tracef("Purging data container on cache %s for topology %d", cacheName, topology.getTopologyId());
            start = timeService.time();
         }
         // We limit it so there is only so many non blocking expiration removals done at the same time
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
                     stage = handleMaxIdleExpireEntry(ice.getKey(), value, maxIdle, false);
                  }
                  stage.whenComplete((obj, t) -> {
                     expirationPermits.add(stage);
                  });
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

   private void printResults(String message, long start, int removedEntries, AtomicInteger errors) {
      if (trace) {
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
            errors.incrementAndGet();
            log.exceptionPurgingDataContainer(e.getCause());
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

   CompletableFuture<Boolean> handleMaxIdleExpireEntry(K key, V value, long maxIdle, boolean isWrite) {
      return handleEitherExpiration(key, value, true, maxIdle, isWrite);
   }

   CompletableFuture<Boolean> removeMaxIdle(AdvancedCache<K, V> cacheToUse, K key, V value) {
      return cacheToUse.removeMaxIdleExpired(key, value);
   }

   private CompletableFuture<Boolean> handleEitherExpiration(K key, V value, boolean maxIdle, long time, boolean isWrite) {
      CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
      CompletableFuture<Boolean> previousFuture = expiring.putIfAbsent(key, completableFuture);
      if (previousFuture == null) {
         if (trace) {
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
      if (isWrite) {
         if (trace) {
            log.tracef("Waiting on prior expiration removal for key %s as this command is a write", key);
         }
         return previousFuture.handle((expired, t) -> {
            if (t != null) {
               Throwable cause = CompletableFutures.extractException(t);
               if (cause instanceof RemoteException) {
                  cause = cause.getCause();
               }
               // Note this exception is from a prior registered stage, not our own. If the prior one got a TimeoutException
               // we try to reregister our write to do a remove expired call. This can happen if a read
               // spawned remove expired times out as it has a 0 lock acquisition timeout. We don't want to propagate
               // the TimeoutException to the caller of the write command as it is unrelated.
               // Note that the remove expired command is never "retried" itself.
               if (cause instanceof org.infinispan.util.concurrent.TimeoutException) {
                  if (trace) {
                     log.tracef("Encountered timeout exception in previous when doing a write - need to retry!");
                  }
                  // Have to try the command ourselves as the prior one timed out (this could be valid if a read based
                  // remove expired couldn't acquire the try lock
                  return handleEitherExpiration(key, value, maxIdle, time, true);
               } else {
                  if (trace) {
                     log.tracef(t, "Encountered exception in previous when doing a write - propagating!");
                  }
                  return CompletableFutures.<Boolean>completedExceptionFuture(cause);
               }
            } else {
               return expired == Boolean.TRUE ? CompletableFutures.completedTrue() : CompletableFutures.completedFalse();
            }
         }).thenCompose(Function.identity());
      }
      if (trace) {
         log.tracef("There is a pending expiration removal for key %s, waiting until it completes.", key);
      }
      // This means there was another thread that found it had expired via max idle or we have optimistic tx
      return previousFuture;
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
      if (expiredMortal) {
         CompletableFuture<Boolean> future = handleLifespanExpireEntry(entry.getKey(), value, lifespan, isWrite);
         if (waitOnLifespanExpiration(isWrite)) {
            return future;
         }
         // We don't want to block the user while the remove expired is happening for lifespan on a read
         return CompletableFutures.completedTrue();
      } else {
         // This means it expired transiently - this will block user until we confirm the entry is okay
         return handleMaxIdleExpireEntry(entry.getKey(), value, entry.getMaxIdle(), isWrite);
      }
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
   public CompletionStage<Void> handleInStoreExpirationInternal(MarshalledEntry<K, V> marshalledEntry) {
      return handleInStoreExpirationInternal(marshalledEntry.getKey(), marshalledEntry);
   }

   private CompletionStage<Void> handleInStoreExpirationInternal(K key, MarshalledEntry<K, V> marshallableEntry) {
      CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
      CompletableFuture<Boolean> previousFuture = expiring.putIfAbsent(key, completableFuture);
      if (previousFuture == null) {
         AdvancedCache<K, V> cacheToUse = cache.withFlags(Flag.SKIP_SHARED_CACHE_STORE, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
         CompletionStage<Boolean> resultStage;
         if (marshallableEntry != null) {
            Metadata metadata = marshallableEntry.getMetadata();
            resultStage = cacheToUse.removeLifespanExpired(key, marshallableEntry.getValue(),
                  metadata.lifespan() == -1 ? null : metadata.lifespan());
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
   protected CompletionStage<Boolean> touchEntryAndReturnIfExpired(InternalCacheEntry ice, int segment) {
      Object key = ice.getKey();
      return attemptTouchAndReturnIfExpired(key, segment)
            .handle((expired, t) -> {
               if (t != null) {
                  Throwable innerT = CompletableFutures.extractException(t);
                  if (innerT instanceof OutdatedTopologyException) {
                     if (trace) {
                        log.tracef("Touch received OutdatedTopologyException, retrying");
                     }
                     return attemptTouchAndReturnIfExpired(key, segment);
                  }
                  return CompletableFutures.<Boolean>completedExceptionFuture(t);
               } else {
                  return CompletableFuture.completedFuture(expired);
               }
            })
            .thenCompose(Function.identity());
   }

   private CompletionStage<Boolean> attemptTouchAndReturnIfExpired(Object key, int segment) {
      LocalizedCacheTopology lct = distributionManager.getCacheTopology();

      TouchCommand touchCommand = cf.running().buildTouchCommand(key, segment);
      touchCommand.setTopologyId(lct.getTopologyId());

      boolean isScattered = configuration.clustering().cacheMode().isScattered();
      DistributionInfo di = lct.getSegmentDistribution(segment);
      // Scattered any node could be a backup, so we have to touch all members
      List<Address> owners = isScattered ? lct.getActualMembers() : di.writeOwners();

      if (touchMode == TouchMode.ASYNC) {
         // Send to all the owners
         rpcManager.sendToMany(owners, touchCommand, DeliverOrder.NONE);
         touchCommand.init(componentRegistry, false);
         return touchCommand.invokeAsync().thenApply(b -> b == Boolean.TRUE ? Boolean.FALSE : Boolean.TRUE);
      }

      CompletionStage<Boolean> remoteStage =
            rpcManager.invokeCommand(owners, touchCommand,
                                     isScattered ? new ScatteredTouchResponseCollector() :
                                     new TouchResponseCollector(),
                                     rpcManager.getSyncRpcOptions());

      touchCommand.init(componentRegistry, false);
      CompletableFuture<Object> localStage = touchCommand.invokeAsync();

      return remoteStage.thenCombine(localStage, (remoteTouch, localTouch) -> {
         if (remoteTouch == Boolean.TRUE && localTouch == Boolean.TRUE) {
            return Boolean.FALSE;
         }
         return Boolean.TRUE;
      });
   }

   private static abstract class AbstractTouchResponseCollector extends ValidResponseCollector<Boolean> {
      @Override
      protected Boolean addTargetNotFound(Address sender) {
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }

      @Override
      protected Boolean addException(Address sender, Exception exception) {
         if (exception instanceof CacheException) {
            throw (CacheException) exception;
         }
         throw new CacheException(exception);
      }
   }

   private static class ScatteredTouchResponseCollector extends AbstractTouchResponseCollector {

      @Override
      public Boolean finish() {
         // No other node was touched
         return Boolean.FALSE;
      }

      @Override
      protected Boolean addValidResponse(Address sender, ValidResponse response) {
         Boolean touched = (Boolean) response.getResponseValue();
         if (touched == Boolean.TRUE) {
            // Return early if any node touched the value - as SCATTERED only exists on a single backup!
            // TODO: what if the read was when one of the backups or primary died?
            return Boolean.TRUE;
         }
         return null;
      }
   }

   private static class TouchResponseCollector extends AbstractTouchResponseCollector {
      @Override
      public Boolean finish() {
         // If all were touched, then the value isn't expired
         return Boolean.TRUE;
      }

      @Override
      protected Boolean addValidResponse(Address sender, ValidResponse response) {
         Boolean touched = (Boolean) response.getResponseValue();
         if (touched == Boolean.FALSE) {
            // Return early if any value wasn't touched!
            return Boolean.FALSE;
         }
         return null;
      }
   }
}
