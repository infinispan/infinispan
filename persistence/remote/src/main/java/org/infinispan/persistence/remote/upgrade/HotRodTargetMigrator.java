package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.DEFAULT_READ_BATCH_SIZE;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.awaitTermination;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.range;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.split;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.supportsIteration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class HotRodTargetMigrator implements TargetMigrator {

   private static final Log log = LogFactory.getLog(HotRodTargetMigrator.class, Log.class);

   public HotRodTargetMigrator() {
   }

   @Override
   public String getName() {
      return "hotrod";
   }

   @Override
   public long synchronizeData(final Cache<Object, Object> cache) throws CacheException {
      return synchronizeData(cache, DEFAULT_READ_BATCH_SIZE, ProcessorInfo.availableProcessors());
   }

   @Override
   public long synchronizeData(Cache<Object, Object> cache, int readBatch, int threads) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      ClassWhiteList whiteList = cache.getCacheManager().getClassWhiteList();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      Set<RemoteStore> stores = loaderManager.getStores(RemoteStore.class);
      String cacheName = cache.getName();
      if (stores.size() != 1) {
         throw log.couldNotMigrateData(cacheName);
      }
      Marshaller marshaller = new MigrationMarshaller(whiteList);
      byte[] knownKeys;
      try {
         knownKeys = marshaller.objectToByteBuffer(MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS);
      } catch (Exception e) {
         throw new CacheException(e);
      }
      RemoteStore store = stores.iterator().next();
      final RemoteCache<Object, Object> remoteSourceCache = store.getRemoteCache();
      if (!supportsIteration(store.getConfiguration().protocolVersion())) {
         if (remoteSourceCache.containsKey(knownKeys)) {
            RemoteStoreConfiguration storeConfig = store.getConfiguration();
            if (!storeConfig.hotRodWrapping()) {
               throw log.remoteStoreNoHotRodWrapping(cacheName);
            }

            Set<Object> keys;
            try {
               keys = (Set<Object>) marshaller.objectFromByteBuffer((byte[]) remoteSourceCache.get(knownKeys));
            } catch (Exception e) {
               throw new CacheException(e);
            }

            ExecutorService es = Executors.newFixedThreadPool(threads);
            final AtomicInteger count = new AtomicInteger(0);
            for (Object okey : keys) {
               final byte[] key = (byte[]) okey;
               es.submit(() -> {
                  try {
                     cache.get(key);
                     int i = count.getAndIncrement();
                     if (log.isDebugEnabled() && i % 100 == 0)
                        log.debugf(">>    Moved %s keys\n", i);
                  } catch (Exception e) {
                     log.keyMigrationFailed(Util.toStr(key), e);
                  }
               });

            }
            awaitTermination(es);
            return count.longValue();
         }
         throw log.missingMigrationData(cacheName);
      } else {
         ClusterExecutor clusterExecutor = cache.getCacheManager().executor()
               .timeout(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
               .singleNodeSubmission();
         CacheTopologyInfo sourceCacheTopologyInfo = remoteSourceCache.getCacheTopologyInfo();
         if (sourceCacheTopologyInfo.getSegmentsPerServer().size() == 1) {
            return migrateFromSingleServer(cache.getCacheManager(), cacheName, readBatch, threads);
         }
         int sourceSegments = sourceCacheTopologyInfo.getNumSegments();
         List<Address> targetServers = cache.getAdvancedCache().getDistributionManager().getWriteConsistentHash().getMembers();

         List<List<Integer>> partitions = split(range(sourceSegments), targetServers.size());
         Iterator<Address> iterator = targetServers.iterator();
         AtomicInteger count = new AtomicInteger();
         TriConsumer<Address, Integer, Throwable> consumer = (a, value, t) -> {
            if (t != null) {
               throw new CacheException(t);
            }
            count.addAndGet(value);
         };

         CompletableFuture<Void>[] futures = new CompletableFuture[partitions.size()];
         int offset = 0;
         for (List<Integer> partition : partitions) {
            Set<Integer> segmentSet = new HashSet<>();
            segmentSet.addAll(partition);

            futures[offset++] = clusterExecutor
                  .filterTargets(Collections.singleton(iterator.next()))
                  .submitConsumer(new MigrationTask(cacheName, segmentSet, readBatch, threads), consumer);
         }
         CompletableFuture.allOf(futures).join();
         return count.get();
      }
   }

   private long migrateFromSingleServer(EmbeddedCacheManager embeddedCacheManager, String cacheName, int readBatch, int threads) {
      MigrationTask migrationTask = new MigrationTask(cacheName, null, readBatch, threads);
      try {
         return migrationTask.apply(embeddedCacheManager);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   @Override
   public void disconnectSource(Cache<Object, Object> cache) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      loaderManager.disableStore(RemoteStore.class.getName());
   }
}
