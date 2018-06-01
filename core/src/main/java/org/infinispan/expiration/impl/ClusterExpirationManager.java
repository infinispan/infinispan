package org.infinispan.expiration.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.expiration.RetrieveLastAccessCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
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
@ThreadSafe
public class ClusterExpirationManager<K, V> extends ExpirationManagerImpl<K, V> {
   private static final Log log = LogFactory.getLog(ClusterExpirationManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final int MAX_ASYNC_EXPIRATIONS = 5;

   @Inject protected AdvancedCache<K, V> cache;
   @Inject protected CommandsFactory cf;
   @Inject protected RpcManager rpcManager;
   @Inject protected DistributionManager distributionManager;

   @Override
   public void start() {
      super.start();
      // Data container entries are retrieved directly, so we don't need to worry about an encodings
      this.cache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache();
   }

   @Override
   public void processExpiration() {
      long start = 0;
      if (!Thread.currentThread().isInterrupted()) {
         try {
            if (trace) {
               log.trace("Purging data container of expired entries");
               start = timeService.time();
            }
            int offset = 0;
            // We limit it so there is only so many async expiration removals done at the same time
            CompletableFuture[] futures = new CompletableFuture[MAX_ASYNC_EXPIRATIONS];
            long currentTimeMillis = timeService.wallClockTime();
            for (Iterator<InternalCacheEntry<K, V>> purgeCandidates = dataContainer.iteratorIncludingExpired();
                 purgeCandidates.hasNext();) {
               InternalCacheEntry<K, V> e = purgeCandidates.next();
               if (e.canExpire()) {
                  // Have to synchronize on the entry to make sure we see the value and metadata at the same time
                  boolean expiredMortal;
                  boolean expiredTransient;
                  V value;
                  long lifespan;
                  long maxIdle;
                  synchronized (e) {
                     value = e.getValue();
                     lifespan = e.getLifespan();
                     maxIdle = e.getMaxIdle();
                     expiredMortal = ExpiryHelper.isExpiredMortal(lifespan, e.getCreated(), currentTimeMillis);
                     expiredTransient = ExpiryHelper.isExpiredTransient(maxIdle, e.getLastUsed(), currentTimeMillis);
                  }
                  // We check lifespan first as this is much less expensive to remove than max idle.
                  if (expiredMortal) {
                     offset = addAndWaitIfFull(handleLifespanExpireEntry(e.getKey(), value, lifespan), futures, offset);
                  } else if (expiredTransient) {
                     offset = addAndWaitIfFull(actualRemoveMaxIdleExpireEntry(e.getKey(), value, maxIdle), futures, offset);
                  }
               }
            }
            if (offset != 0) {
               // Make sure that all of the futures are complete before returning
               for (int i = 0; i < offset; ++i) {
                  futures[i].join();
               }
            }
            if (trace) {
               log.tracef("Purging data container completed in %s",
                       Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
            }
         } catch (Exception e) {
            log.exceptionPurgingDataContainer(e);
         }
      }

      if (!Thread.currentThread().isInterrupted()) {
         persistenceManager.purgeExpired();
      }
   }

   private int addAndWaitIfFull(CompletableFuture future, CompletableFuture[] futures, int offset) {
      futures[offset++] = future;
      if (offset == futures.length) {
         // Wait for them to complete
         CompletableFuture.allOf(futures).join();
         Arrays.fill(futures, null);
         offset = 0;
      }
      return offset;
   }

   CompletableFuture<Void> handleLifespanExpireEntry(K key, V value, long lifespan) {
      // The most used case will be a miss so no extra read before
      if (expiring.putIfAbsent(key, key) == null) {
         if (trace) {
            log.tracef("Submitting expiration removal for key %s which had lifespan of %s", toStr(key), lifespan);
         }
         CompletableFuture<Void> future = cache.removeLifespanExpired(key, value, lifespan);
         return future.whenComplete((v, t) -> expiring.remove(key, key));
      }
      return CompletableFutures.completedNull();
   }

   // Method invoked when an entry is found to be expired via get
   CompletableFuture<Boolean> handleMaxIdleExpireEntry(K key, V value, long maxIdle) {
      return actualRemoveMaxIdleExpireEntry(key, value, maxIdle);
   }

   // Method invoked when entry should be attempted to be removed via max idle
   CompletableFuture<Boolean> actualRemoveMaxIdleExpireEntry(K key, V value, long maxIdle) {
      CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
      Object expiringObject = expiring.putIfAbsent(key, completableFuture);
      if (expiringObject == null) {
         if (trace) {
            log.tracef("Submitting expiration removal for key %s which had maxIdle of %s", toStr(key), maxIdle);
         }
         completableFuture.whenComplete((b, t) -> expiring.remove(key, completableFuture));
         try {
            CompletableFuture<Boolean> expired = cache.removeMaxIdleExpired(key, value);
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
   public CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime) {
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
         handleLifespanExpireEntry(entry.getKey(), value, lifespan);
         // We don't want to block the user while the remove expired is happening for lifespan
         return CompletableFutures.completedTrue();
      } else {
         // This means it expired transiently - this will block user until we confirm the entry is okay
         return handleMaxIdleExpireEntry(entry.getKey(), value, entry.getMaxIdle());
      }
   }

   @Override
   public CompletableFuture<Boolean> entryExpiredInMemoryFromIteration(InternalCacheEntry<K, V> entry, long currentTime) {
      // We need to synchronize on the entry since {@link InternalCacheEntry} locks the entry when doing an update
      // so we can see both the new value and the metadata
      boolean expiredTransient;
      synchronized (entry) {
         expiredTransient = ExpiryHelper.isExpiredTransient(entry.getMaxIdle(), entry.getLastUsed(), currentTime);
      }
      if (expiredTransient) {
         // Max idle expiration - we just return it (otherwise we would have to incur remote overhead)
         // This entry will be removed on next get or reaper running
         return CompletableFutures.completedFalse();
      } else {
         // Lifespan was expired - but we don't want to take the hit of causing an expire command to be fired
         return CompletableFutures.completedTrue();
      }
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
   public void handleInStoreExpiration(MarshalledEntry<K, V> marshalledEntry) {
      K key = marshalledEntry.getKey();
      if (expiring.putIfAbsent(key, key) == null) {
         try {
            InternalMetadata metadata = marshalledEntry.getMetadata();
            cache.removeLifespanExpired(key, marshalledEntry.getValue(), metadata.lifespan() == -1 ? null : metadata.lifespan())
                  .join();
         } finally {
            expiring.remove(key, key);
         }
      }
   }

   @Override
   public CompletableFuture<Long> retrieveLastAccess(Object key, Object value) {
      Long access = localLastAccess(key, value);

      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      DistributionInfo info = topology.getDistribution(key);

      if (trace) {
         log.tracef("Asking all read owners %s for key: %s - for latest access time", info.readOwners(), key);
      }

      // Need to gather last access times
      RetrieveLastAccessCommand rlac = cf.buildRetrieveLastAccessCommand(key, value);
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
