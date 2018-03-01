package org.infinispan.persistence.rest.upgrade;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.rest.RestStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * This migrator should no longer be used and the HotRodTargetMigrator should be used instead
 */
@MetaInfServices
@Deprecated
public class RestTargetMigrator implements TargetMigrator {
   private static final Log log = LogFactory.getLog(RestTargetMigrator.class, Log.class);

   public RestTargetMigrator() {
   }

   @Override
   public String getName() {
      return "rest";
   }

   @Override
   public long synchronizeData(final Cache<Object, Object> cache) throws CacheException {
      return synchronizeData(cache, 0, ProcessorInfo.availableProcessors());

   }

   @Override
   public long synchronizeData(Cache<Object, Object> cache, int readBatch, int threads) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      Set<RestStore> stores = loaderManager.getStores(RestStore.class);

      final AtomicInteger count = new AtomicInteger(0);
      for (RestStore store : stores) {

         Set<Object> keys;
         try {
            keys = PersistenceUtil.toKeySet(store, null);
         } catch (PersistenceException e) {
            throw new CacheException(e);
         }
         ExecutorService es = Executors.newFixedThreadPool(threads);

         for (final Object key : keys) {
            es.submit(new Runnable() {
               @Override
               public void run() {
                  try {
                     cache.get(key);
                     int i = count.getAndIncrement();
                     if (log.isDebugEnabled() && i % 100 == 0)
                        log.debugf(">>    Moved %s keys\n", i);
                  } catch (Exception e) {
                     if ((key instanceof String) && ((String) key).matches("___MigrationManager_.+_KnownKeys___")) {
                        // ISPN-3724 Ignore keys from other migrators.
                     } else {
                        log.keyMigrationFailed(Util.toStr(key), e);
                     }
                  }
               }
            });

         }
         es.shutdown();
         try {
            while (!es.awaitTermination(500, TimeUnit.MILLISECONDS)) {
            }
         } catch (InterruptedException e) {
            throw new CacheException(e);
         }

      }
      return count.longValue();
   }

   @Override
   public void disconnectSource(Cache<Object, Object> cache) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      loaderManager.disableStore(RestStore.class.getName());
   }
}
