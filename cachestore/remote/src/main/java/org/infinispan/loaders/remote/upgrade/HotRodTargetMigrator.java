package org.infinispan.loaders.remote.upgrade;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.remote.RemoteCacheStore;
import org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfiguration;
import org.infinispan.loaders.remote.logging.Log;
import org.infinispan.upgrade.TargetMigrator;
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
      CacheLoaderManager loaderManager = cr.getComponent(CacheLoaderManager.class);
      List<RemoteCacheStore> stores = loaderManager.getCacheLoaders(RemoteCacheStore.class);
      Marshaller marshaller = new GenericJBossMarshaller();
      byte[] knownKeys;
      try {
         knownKeys = marshaller.objectToByteBuffer(MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS);
      } catch (Exception e) {
         throw new CacheException(e);
      }

      for (RemoteCacheStore store : stores) {
         final RemoteCache<Object, Object> storeCache = store.getRemoteCache();
         if (storeCache.containsKey(knownKeys)) {
            RemoteCacheStoreConfiguration storeConfig = (RemoteCacheStoreConfiguration) store.getConfiguration();
            if (!storeConfig.hotRodWrapping()) {
               throw log.remoteStoreNoHotRodWrapping(cache.getName());
            }


            Set<byte[]> keys;
            try {
               keys = (Set<byte[]>) marshaller.objectFromByteBuffer((byte[])storeCache.get(knownKeys));
            } catch (Exception e) {
               throw new CacheException(e);
            }

            ExecutorService es = Executors.newFixedThreadPool(threads);
            final AtomicInteger count = new AtomicInteger(0);
            for (final byte[] key : keys) {
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
      CacheLoaderManager loaderManager = cr.getComponent(CacheLoaderManager.class);
      loaderManager.disableCacheStore(RemoteCacheStore.class.getName());
   }
}
