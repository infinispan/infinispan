package org.infinispan.eviction.impl;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationManagerImpl implements PassivationManager {
   private static final Log log = LogFactory.getLog(PassivationManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private PersistenceManager persistenceManager;
   @Inject private CacheNotifier notifier;
   @Inject private Configuration cfg;
   @Inject private InternalDataContainer<Object, Object> container;
   @Inject private TimeService timeService;
   @Inject private MarshallableEntryFactory marshalledEntryFactory;
   @Inject private DistributionManager distributionManager;
   @Inject private KeyPartitioner keyPartitioner;

   private volatile boolean skipOnStop = false;

   boolean statsEnabled = false;
   boolean enabled = false;

   private final AtomicLong passivations = new AtomicLong(0);

   @Start(priority = 12)
   public void start() {
      enabled = cfg.persistence().passivation() && cfg.persistence().usingStores();
      if (enabled) {
         statsEnabled = cfg.jmxStatistics().enabled();
      }
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   private boolean isL1Key(Object key) {
      return distributionManager != null && !distributionManager.getCacheTopology().isWriteOwner(key);
   }

   @Override
   public void passivate(InternalCacheEntry entry) {
      Object key;
      if (enabled && entry != null && !isL1Key(key = entry.getKey())) {
         // notify listeners that this entry is about to be passivated
         notifier.notifyCacheEntryPassivated(key, entry.getValue(), true,
               ImmutableContext.INSTANCE, null);
         if (trace) log.tracef("Passivating entry %s", toStr(key));
         try {
            MarshallableEntry marshalledEntry = marshalledEntryFactory.create(key, entry.getValue(), entry.getMetadata(),
                  entry.getExpiryTime(), entry.getLastUsed());
            persistenceManager.writeToAllNonTxStores(marshalledEntry, keyPartitioner.getSegment(key), BOTH);
            if (statsEnabled) passivations.getAndIncrement();
         } catch (CacheException e) {
            log.unableToPassivateEntry(key, e);
         }
         notifier.notifyCacheEntryPassivated(key, null, false,
               ImmutableContext.INSTANCE, null);
      }
   }

   @Override
   public void passivateAll() throws PersistenceException {
      if (enabled && !skipOnStop) {
         long start = timeService.time();
         log.passivatingAllEntries();

         int count = container.sizeIncludingExpired();
         Iterable<MarshallableEntry> iterable = () -> new IteratorMapper<>(container.iterator(), e ->
            marshalledEntryFactory.create(e.getKey(), e.getValue(), e.getMetadata(), e.getExpiryTime(), e.getLastUsed()));
         persistenceManager.writeBatchToAllNonTxStores(iterable, BOTH, 0);
         log.passivatedEntries(count, Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
      }
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
