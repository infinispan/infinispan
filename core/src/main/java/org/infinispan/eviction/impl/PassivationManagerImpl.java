package org.infinispan.eviction.impl;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.persistence.manager.PassivationPersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationManagerImpl extends AbstractPassivationManager {
   private static final Log log = LogFactory.getLog(PassivationManagerImpl.class);

   @Inject PersistenceManager persistenceManager;
   @Inject CacheNotifier<Object, Object> notifier;
   @Inject Configuration cfg;
   @Inject InternalDataContainer<Object, Object> container;
   @Inject TimeService timeService;
   @Inject MarshallableEntryFactory<?, ?> marshalledEntryFactory;
   @Inject DistributionManager distributionManager;
   @Inject KeyPartitioner keyPartitioner;

   PassivationPersistenceManager passivationPersistenceManager;

   private volatile boolean skipOnStop = false;

   boolean statsEnabled = false;
   boolean enabled = false;

   private final AtomicLong passivations = new AtomicLong(0);

   @Start(priority = 12)
   public void start() {
      enabled = !persistenceManager.isReadOnly() && cfg.persistence().passivation() && cfg.persistence().usingStores();
      if (enabled) {
         passivationPersistenceManager = (PassivationPersistenceManager) persistenceManager;
         statsEnabled = cfg.statistics().enabled();
      }
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   private boolean isL1Key(Object key) {
      return distributionManager != null && !distributionManager.getCacheTopology().isWriteOwner(key);
   }

   private CompletionStage<Boolean> doPassivate(Object key, InternalCacheEntry<?, ?> entry) {
      if (log.isTraceEnabled()) log.tracef("Passivating entry %s", toStr(key));
      MarshallableEntry<?, ?> marshalledEntry = marshalledEntryFactory.create((InternalCacheEntry) entry);
      CompletionStage<Void> stage = passivationPersistenceManager.passivate(marshalledEntry, keyPartitioner.getSegment(key));
         return stage.handle((v, t) -> {
            if (t != null) {
               CONTAINER.unableToPassivateEntry(key, t);
               return false;
            }
            if (statsEnabled) {
               passivations.getAndIncrement();
            }
            return true;
         });
   }

   @Override
   public CompletionStage<Void> passivateAsync(InternalCacheEntry<?, ?> entry) {
      Object key;
      if (enabled && entry != null && !isL1Key(key = entry.getKey())) {
         if (notifier.hasListener(CacheEntryPassivated.class)) {
            return notifier.notifyCacheEntryPassivated(key, entry.getValue(), true, ImmutableContext.INSTANCE, null)
                  .thenCompose(v -> doPassivate(key, entry))
                  .thenCompose(v -> notifier.notifyCacheEntryPassivated(key, null, false, ImmutableContext.INSTANCE, null));
         } else {
            return CompletionStages.ignoreValue(doPassivate(key, entry));
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public void passivateAll() throws PersistenceException {
      CompletionStages.join(passivateAllAsync());
   }

   @Override
   public CompletionStage<Void> passivateAllAsync() throws PersistenceException {
      if (!enabled || skipOnStop)
         return CompletableFutures.completedNull();

      long start = timeService.time();
      if (CONTAINER.isDebugEnabled()) {
         CONTAINER.debug("Passivating all entries to disk");
      }

      int count = container.sizeIncludingExpired();
      Iterable<MarshallableEntry<Object, Object>> iterable = () -> new IteratorMapper<>(container.iterator(), e -> marshalledEntryFactory.create((InternalCacheEntry) e));
      return persistenceManager.writeEntries(iterable, PRIVATE)
                               .thenRun(() -> {
                                  long durationMillis = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
                                  if (CONTAINER.isDebugEnabled()) {
                                     CONTAINER.debugf("Passivated %d entries in %s", count, Util.prettyPrintTime(durationMillis));
                                  }
                               });
   }

   @Override
   public void skipPassivationOnStop(boolean skip) {
      this.skipOnStop = skip;
   }

   @Override
   public long getPassivations() {
      return passivations.get();
   }

   @Override
   public boolean getStatisticsEnabled() {
      return statsEnabled;
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      statsEnabled = enabled;
   }

   @Override
   public void resetStatistics() {
      passivations.set(0L);
   }
}
