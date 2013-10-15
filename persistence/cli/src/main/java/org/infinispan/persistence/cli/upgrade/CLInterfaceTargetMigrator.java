package org.infinispan.persistence.cli.upgrade;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.cli.CLInterfaceLoader;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class CLInterfaceTargetMigrator implements TargetMigrator {

   private static final Log log = LogFactory.getLog(CLInterfaceTargetMigrator.class);

   private static final String KNOWN_KEY = "___MigrationManager_CLI_KnownKeys___";

   private ObjectMapper jsonMapper = new ObjectMapper().enableDefaultTyping(
         ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.WRAPPER_OBJECT);

   @Override
   public String getName() {
      return "cli";
   }

   @Override
   public long synchronizeData(final Cache<Object, Object> cache) throws CacheException {
      int threads = Runtime.getRuntime().availableProcessors();
      PersistenceManager loaderManager = getPersistenceManager(cache);
      Set<CLInterfaceLoader> loaders = loaderManager.getStores(CLInterfaceLoader.class);

      for (CLInterfaceLoader loader : loaders) {
         MarshalledEntry loadedKnownKey = loader.load(KNOWN_KEY);
         if (loadedKnownKey != null) {
            Set<Object> keys;
            try {
               keys = jsonMapper
                     .readValue((String) loadedKnownKey.getValue(), HashSet.class);
            } catch (IOException e) {
               throw new CacheException(
                     "Unable to read JSON value: " + loadedKnownKey.getValue(), e);
            }

            ExecutorService es = Executors.newFixedThreadPool(threads);
            final AtomicInteger count = new AtomicInteger(0);
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

   private PersistenceManager getPersistenceManager(Cache<Object, Object> cache) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      return cr.getComponent(PersistenceManager.class);
   }

   @Override
   public void disconnectSource(Cache<Object, Object> cache) throws CacheException {
      PersistenceManager loaderManager = getPersistenceManager(cache);
      loaderManager.disableStore(CLInterfaceLoader.class.getName());
   }

}
