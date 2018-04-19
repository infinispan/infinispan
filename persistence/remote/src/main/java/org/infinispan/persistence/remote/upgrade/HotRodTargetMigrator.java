package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.DEFAULT_READ_BATCH_SIZE;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.awaitTermination;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.range;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.split;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.supportsIteration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.upgrade.TargetMigrator;
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
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      Set<RemoteStore> stores = loaderManager.getStores(RemoteStore.class);
      if (stores.size() != 1) {
         throw log.couldNotMigrateData(cache.getName());
      }
      Marshaller marshaller = new MigrationMarshaller();
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
               throw log.remoteStoreNoHotRodWrapping(cache.getName());
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
         throw log.missingMigrationData(cache.getName());
      } else {
         DistributedExecutorService executor = new DefaultExecutorService(cache);
         try {
            CacheTopologyInfo sourceCacheTopologyInfo = remoteSourceCache.getCacheTopologyInfo();
            if (sourceCacheTopologyInfo.getSegmentsPerServer().size() == 1) {
               return migrateFromSingleServer(cache, readBatch, threads);
            }
            int sourceSegments = sourceCacheTopologyInfo.getNumSegments();
            List<Address> targetServers = cache.getAdvancedCache().getDistributionManager().getWriteConsistentHash().getMembers();

            List<List<Integer>> partitions = split(range(sourceSegments), targetServers.size());
            Iterator<Address> iterator = targetServers.iterator();
            List<CompletableFuture<Integer>> futures = new ArrayList<>(targetServers.size());
            for (List<Integer> partition : partitions) {
               Set<Integer> segmentSet = new HashSet<>();
               segmentSet.addAll(partition);
               DistributedTask<Integer> task = executor
                     .createDistributedTaskBuilder(new MigrationTask(segmentSet, readBatch, threads))
                     .timeout(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
                     .build();
               futures.add(executor.submit(iterator.next(), task));
            }
            return futures.stream().mapToInt(f -> {
               try {
                  return f.get();
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw log.couldNotMigrateData(cache.getName());
               } catch (ExecutionException e) {
                  throw new CacheException(e);
               }
            }).sum();

         } finally {
            executor.shutdownNow();
         }
      }
   }

   private long migrateFromSingleServer(Cache<Object, Object> cache, int readBatch, int threads) {
      MigrationTask migrationTask = new MigrationTask(null, readBatch, threads);
      migrationTask.setEnvironment(cache, null);
      try {
         return migrationTask.call();
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
