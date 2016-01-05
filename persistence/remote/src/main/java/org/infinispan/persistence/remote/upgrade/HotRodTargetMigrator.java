package org.infinispan.persistence.remote.upgrade;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.Futures;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.*;

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
      return synchronizeData(cache, DEFAULT_READ_BATCH_SIZE, Runtime.getRuntime().availableProcessors());
   }

   @Override
   public long synchronizeData(Cache<Object, Object> cache, int readBatch, int threads) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      Set<RemoteStore> stores = loaderManager.getStores(RemoteStore.class);
      Marshaller marshaller = new MigrationMarshaller();
      byte[] knownKeys;
      try {
         knownKeys = marshaller.objectToByteBuffer(MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS);
      } catch (Exception e) {
         throw new CacheException(e);
      }
      for (RemoteStore store : stores) {
         final RemoteCache<Object, Object> storeCache = store.getRemoteCache();
         if (!supportsIteration(store.getConfiguration().protocolVersion())) {
            if (storeCache.containsKey(knownKeys)) {
               RemoteStoreConfiguration storeConfig = store.getConfiguration();
               if (!storeConfig.hotRodWrapping()) {
                  throw log.remoteStoreNoHotRodWrapping(cache.getName());
               }

               Set<Object> keys;
               try {
                  keys = (Set<Object>) marshaller.objectFromByteBuffer((byte[]) storeCache.get(knownKeys));
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
               gracefulShutdown(es);
               return count.longValue();
            }
            throw log.missingMigrationData(cache.getName());

         } else {

            CacheTopologyInfo cacheTopologyInfo = storeCache.getCacheTopologyInfo();
            Map<SocketAddress, Set<Integer>> remoteServers = cacheTopologyInfo.getSegmentsPerServer();
            if (remoteServers.size() == 1) {
               final AtomicInteger count = new AtomicInteger(0);
               ExecutorService es = Executors.newFixedThreadPool(threads);
               migrateEntriesWithMetadata(storeCache, cache, es, knownKeys, count, null, readBatch);
               gracefulShutdown(es);
               return count.get();
            } else {
               int numSegments = cacheTopologyInfo.getNumSegments();
               List<Address> servers = cache.getAdvancedCache().getDistributionManager().getConsistentHash().getMembers();
               List<List<Integer>> partitions = split(range(numSegments), servers.size());
               DistributedExecutorService executor = new DefaultExecutorService(cache);
               Iterator<Address> iterator = servers.iterator();
               ArrayList<NotifyingFuture<Integer>> futures = new ArrayList<>(servers.size());
               for (List<Integer> partition : partitions) {
                  Set<Integer> segmentSet = new HashSet<>();
                  segmentSet.addAll(partition);
                  futures.add(executor.submit(iterator.next(), new MigrationTask(segmentSet, readBatch, threads)));
               }
               NotifyingFuture<List<Integer>> result = Futures.combine(futures);
               try {
                  List<Integer> integers = result.get();
                  int sum = 0;
                  for (Integer i : integers) {
                     sum += i;
                  }
                  return sum;
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               } catch (ExecutionException e) {
                  throw new CacheException(e);
               }
            }
         }
      }
      throw log.couldNotMigrateData(cache.getName());
   }


   @Override
   public void disconnectSource(Cache<Object, Object> cache) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      loaderManager.disableStore(RemoteStore.class.getName());
   }
}


