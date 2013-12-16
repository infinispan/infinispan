package org.infinispan.persistence.remote.upgrade;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.logging.LogFactory;

public class HotRodTargetMigrator implements TargetMigrator {
   private static final String MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS = "___MigrationManager_HotRod_KnownKeys___";

   private static final Log log = LogFactory.getLog(HotRodTargetMigrator.class, Log.class);

   public HotRodTargetMigrator() {
   }

   @Override
   public String getName() {
      return "hotrod";
   }

   @Override
   public long synchronizeData(final Cache<Object, Object> cache) throws CacheException {
      int threads = Runtime.getRuntime().availableProcessors();
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
         if (storeCache.containsKey(knownKeys)) {
            RemoteStoreConfiguration storeConfig = store.getConfiguration();
            if (!storeConfig.hotRodWrapping()) {
               throw log.remoteStoreNoHotRodWrapping(cache.getName());
            }


            Set<Object> keys;
            try {
               keys = (Set<Object>) marshaller.objectFromByteBuffer((byte[])storeCache.get(knownKeys));
            } catch (Exception e) {
               throw new CacheException(e);
            }

            ExecutorService es = Executors.newFixedThreadPool(threads);
            final AtomicInteger count = new AtomicInteger(0);
            for (Object okey : keys) {
               final byte[] key = (okey instanceof ByteArrayKey) ? ((ByteArrayKey)okey).getData() : ((byte[])okey);
               es.submit(new Runnable() {

                  @Override
                  public void run() {
                     try {
                        cache.get(key);
                        int i = count.getAndIncrement();
                        if (log.isDebugEnabled() && i % 100 == 0)
                           log.debugf(">>    Moved %s keys\n", i);
                     } catch (Exception e) {
                        log.keyMigrationFailed(Util.toStr(key), e);
                     }
                  }
               });

            }
            es.shutdown();
            try {
               while (!es.awaitTermination(500, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
               throw new CacheException(e);
            }
            return count.longValue();
         }
      }
      throw log.missingMigrationData(cache.getName());
   }

   @Override
   public void disconnectSource(Cache<Object, Object> cache) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      loaderManager.disableStore(RemoteStore.class.getName());
   }
}
