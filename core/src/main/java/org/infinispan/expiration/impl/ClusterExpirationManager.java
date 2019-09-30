package org.infinispan.expiration.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.expiration.RetrieveLastAccessCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;
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
   private static final boolean trace = log.isTraceEnabled();

   /**
    * Defines the maximum number of allowed concurrent expirations. Any expirations over this must wait until
    * another has completed before processing
    */
   private static final int MAX_CONCURRENT_EXPIRATIONS = 100;

   @Inject protected ComponentRef<AdvancedCache<K, V>> cacheRef;
   @Inject protected ComponentRef<CommandsFactory> cf;
   @Inject protected RpcManager rpcManager;
   @Inject protected DistributionManager distributionManager;

   private AdvancedCache<K, V> cache;
   private Address localAddress;
   private long timeout;
   private String cacheName;

   @Override
   public void start() {
      super.start();
      // Data container entries are retrieved directly, so we don't need to worry about an encodings
      this.cache = AbstractDelegatingCache.unwrapCache(cacheRef.wired()).getAdvancedCache();
      this.cacheName = cache.getName();
      this.localAddress = cache.getCacheManager().getAddress();
      this.timeout = cache.getCacheConfiguration().clustering().remoteTimeout();
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

         IntSet segments = IntSets.from(topology.getReadConsistentHash().getPrimarySegmentsForOwner(localAddress));

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
                     stage = actualRemoveMaxIdleExpireEntry(ice.getKey(), value, maxIdle, false);
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
   CompletableFuture<Void> handleLifespanExpireEntry(K key, V value, long lifespan, boolean skipLocking) {
      // The most used case will be a miss so no extra read before
      if (expiring.putIfAbsent(key, key) == null) {
         if (trace) {
            log.tracef("Submitting expiration removal for key %s which had lifespan of %s", toStr(key), lifespan);
         }
         AdvancedCache<K, V> cacheToUse = skipLocking ? cache.withFlags(Flag.SKIP_LOCKING) : cache;
         CompletableFuture<Void> future = cacheToUse.removeLifespanExpired(key, value, lifespan);
         return future.whenComplete((v, t) -> expiring.remove(key, key));
      }
      return CompletableFutures.completedNull();
   }

   // Method invoked when an entry is found to be expired via get
   CompletableFuture<Boolean> handleMaxIdleExpireEntry(K key, V value, long maxIdle, boolean skipLocking) {
      return actualRemoveMaxIdleExpireEntry(key, value, maxIdle, skipLocking);
   }

   // Method invoked when entry should be attempted to be removed via max idle
   CompletableFuture<Boolean> actualRemoveMaxIdleExpireEntry(K key, V value, long maxIdle, boolean skipLocking) {
      CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
      Object expiringObject = expiring.putIfAbsent(key, completableFuture);
      if (expiringObject == null) {
         if (trace) {
            log.tracef("Submitting expiration removal for key %s which had maxIdle of %s", toStr(key), maxIdle);
         }
         completableFuture.whenComplete((b, t) -> expiring.remove(key, completableFuture));
         try {
            AdvancedCache<K, V> cacheToUse = skipLocking ? cache.withFlags(Flag.SKIP_LOCKING) : cache;
            CompletableFuture<Boolean> expired = cacheToUse.removeMaxIdleExpired(key, value);
            expired.whenComplete((b, t) -> {
               if (t != null) {
                  completableFuture.completeExceptionally(t);
               } else {
                  completableFuture.complete(b);
               }
            });
            return completableFuture;
         } catch (Throwable t) {
            completableFuture.completeExceptionally(t);
            throw t;
         }
      } else if (expiringObject instanceof CompletableFuture) {
         // This means there was another thread that found it had expired via max idle
         return (CompletableFuture<Boolean>) expiringObject;
      } else {
         // If it wasn't a CompletableFuture we had a lifespan removal occurring so it will be removed for sure
         return CompletableFutures.completedTrue();
      }
   }

   @Override
   public CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime,
         boolean hasLock) {
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
         CompletableFuture<Void> future = handleLifespanExpireEntry(entry.getKey(), value, lifespan, hasLock);
         if (hasLock) {
            return future.thenCompose(CompletableFutures.composeTrue());
         }
         // We don't want to block the user while the remove expired is happening for lifespan
         return CompletableFutures.completedTrue();
      } else {
         // This means it expired transiently - this will block user until we confirm the entry is okay
         return handleMaxIdleExpireEntry(entry.getKey(), value, entry.getMaxIdle(), hasLock);
      }
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
   public void handleInStoreExpiration(K key) {
      if (expiring.putIfAbsent(key, key) == null) {
         // Unfortunately stores don't pull the entry so we can't tell exactly why it expired and thus we have to remove
         // the entire value.  Unfortunately this could cause a concurrent write to be undone
         try {
            cache.removeLifespanExpired(key, null, null).join();
         } finally {
            expiring.remove(key, key);
         }
      }
   }

   @Override
   public void handleInStoreExpiration(MarshallableEntry<K, V> marshalledEntry) {
      K key = marshalledEntry.getKey();
      if (expiring.putIfAbsent(key, key) == null) {
         try {
            Metadata metadata = marshalledEntry.getMetadata();
            cache.removeLifespanExpired(key, marshalledEntry.getValue(), metadata.lifespan() == -1 ? null : metadata.lifespan())
                  .join();
         } finally {
            expiring.remove(key, key);
         }
      }
   }

   @Override
   public CompletableFuture<Long> retrieveLastAccess(Object key, Object value, int segment) {
      Long access = localLastAccess(key, value, segment);

      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      DistributionInfo info = topology.getDistribution(key);

      if (trace) {
         log.tracef("Asking all read owners %s for key: %s - for latest access time", info.readOwners(), key);
      }

      // Need to gather last access times
      RetrieveLastAccessCommand rlac = cf.running().buildRetrieveLastAccessCommand(key, value, segment);
      rlac.setTopologyId(topology.getTopologyId());

      // In scattered cache read owners will only contain primary
      return rpcManager.invokeCommand(info.readOwners(), rlac, new MaxResponseCollector<>(access),
            rpcManager.getSyncRpcOptions()).toCompletableFuture();
   }

   static class MaxResponseCollector<T extends Comparable<T>> extends ValidResponseCollector<T> {
      T highest;

      MaxResponseCollector(T highest) {
         this.highest = highest;
      }

      @Override
      public T finish() {
         return highest;
      }

      @Override
      protected T addValidResponse(Address sender, ValidResponse response) {
         T value = (T) response.getResponseValue();
         if (value != null && (highest == null || highest.compareTo(value) < 0)) {
            highest = value;
         }
         return null;
      }

      @Override
      protected T addTargetNotFound(Address sender) {
         // We don't care about a node leaving
         return null;
      }

      @Override
      protected T addException(Address sender, Exception exception) {
         throw ResponseCollectors.wrapRemoteException(sender, exception);
      }
   }
}
