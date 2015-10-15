package org.infinispan.eviction.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.persistence.PersistenceUtil.internalMetadata;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

public class PassivationManagerImpl implements PassivationManager {

   PersistenceManager persistenceManager;
   CacheNotifier notifier;
   Configuration cfg;

   boolean statsEnabled = false;
   boolean enabled = false;
   private static final Log log = LogFactory.getLog(PassivationManagerImpl.class);
   private final AtomicLong passivations = new AtomicLong(0);
   private DataContainer<Object, Object> container;
   private TimeService timeService;
   private static final boolean trace = log.isTraceEnabled();
   private MarshalledEntryFactory marshalledEntryFactory;
   private DistributionManager distributionManager;

   @Inject
   public void inject(PersistenceManager persistenceManager, CacheNotifier notifier, Configuration cfg, DataContainer container,
                      TimeService timeService, MarshalledEntryFactory marshalledEntryFactory,
                      DistributionManager distributionManager) {
      this.persistenceManager = persistenceManager;
      this.notifier = notifier;
      this.cfg = cfg;
      this.container = container;
      this.timeService = timeService;
      this.marshalledEntryFactory = marshalledEntryFactory;
      this.distributionManager = distributionManager;
   }

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
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }

   @Override
   public void passivate(InternalCacheEntry entry) {
      Object key;
      if (enabled && entry != null && !isL1Key(key = entry.getKey())) {
         // notify listeners that this entry is about to be passivated
         notifier.notifyCacheEntryPassivated(key, entry.getValue(), true,
               ImmutableContext.INSTANCE, null);
         if (trace) log.tracef("Passivating entry %s", key);
         try {
            MarshalledEntry marshalledEntry = marshalledEntryFactory.newMarshalledEntry(entry.getKey(), entry.getValue(),
                                                                                        internalMetadata(entry));
            persistenceManager.writeToAllStores(marshalledEntry, BOTH);
            if (statsEnabled) passivations.getAndIncrement();
         } catch (CacheException e) {
            log.unableToPassivateEntry(key, e);
         }
         notifier.notifyCacheEntryPassivated(key, null, false,
               ImmutableContext.INSTANCE, null);
      }
   }

   @Override
   @Stop(priority = 9)
   public void passivateAll() throws PersistenceException {
      if (enabled) {
         long start = timeService.time();
         log.passivatingAllEntries();
         for (InternalCacheEntry e : container) {
            if (trace) log.tracef("Passivating %s", e.getKey());
            persistenceManager.writeToAllStores(marshalledEntryFactory.newMarshalledEntry(e.getKey(), e.getValue(),
                                                                        internalMetadata(e)), BOTH);
         }
         log.passivatedEntries(container.size(),
                               Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
      }
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
