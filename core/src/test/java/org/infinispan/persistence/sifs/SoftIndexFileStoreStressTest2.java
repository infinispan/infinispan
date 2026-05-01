package org.infinispan.persistence.sifs;

import java.io.File;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * This test is copied from https://github.com/hmlnarik/ispn-test-mvn and repurposed to be used by ISPN with TestNG
 */
@org.testng.annotations.Test(groups = "stress", testName = "persistence.sifs.SoftIndexFileStoreStressTest2")
public class SoftIndexFileStoreStressTest2 {
   private static final String CACHE_STORE_ROOT_DIR =
         System.getProperty("cache.dir", System.getProperty("java.io.tmpdir") + File.separator + "infinispan-sifs-" + System.currentTimeMillis());
   private static final int INITIAL_SIZE = 50000;
   private static final int N_THREADS = Runtime.getRuntime().availableProcessors() * 2;
   private static final int MAXIMUM_ATTEMPTS = 1000_000;
   private static final long MAX_PAGES = 400;
   private static final String KEY_PREFIX = "RBROOT/replicas_eu.test0.test1.acme.cache.entity.CacheableEntity_cachingnamespace/DUMMY_SEGMENT:page";
   private static final Logger LOG = LogManager.getLogger(SoftIndexFileStoreStressTest2.class);
   public static final int TEST_MAX_TIME_SECONDS = 600;

   public static String getCacheStoreRootDir() {
      return CACHE_STORE_ROOT_DIR;
   }

   private static final Random random = new Random();

   private final AtomicInteger COUNTER = new AtomicInteger();

   /**
    * Copied from IfspnReplicationBuffer#initializeCacheConfig()
    *
    * @return
    */
   public static ConfigurationBuilder initializeCacheConfig() {
      ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
      cacheConfig.clustering().cacheMode(CacheMode.LOCAL);
      cacheConfig.invocationBatching().enable();
      cacheConfig.persistence().clearStores();

      if (getCacheStoreRootDir() != null) {
         // LOGGER.infov("Configuring RB soft-indexed file store: {}", getCacheStoreRootDir());
         cacheConfig.persistence().passivation(false).addSoftIndexFileStore().shared(false).dataLocation(getCacheStoreRootDir() + "/replicator")
               .indexLocation(getCacheStoreRootDir() + "/replicator").ignoreModifications(false).preload(true).purgeOnStartup(false);
      } else {
         // LOGGER.info("Configuring RB as non-persistent");
         cacheConfig.memory().maxCount(Long.MAX_VALUE).storage(StorageType.HEAP);
      }

      // Custom modifications
      updateCacheConfigPessimistic(cacheConfig);

      return cacheConfig;
   }

   private static void updateCacheConfigSerializablePessimistic(final ConfigurationBuilder cacheConfig) {
      cacheConfig.invocationBatching().disable();
      cacheConfig.locking().isolationLevel(IsolationLevel.SERIALIZABLE);
      cacheConfig.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .cacheStopTimeout(3, TimeUnit.SECONDS)
      ;
   }

   private static void updateCacheConfigRepeatableReadPessimistic(final ConfigurationBuilder cacheConfig) {
      cacheConfig.invocationBatching().disable();
      cacheConfig.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      cacheConfig.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .cacheStopTimeout(3, TimeUnit.SECONDS)
      ;
   }

   private static void updateCacheConfigPessimistic(final ConfigurationBuilder cacheConfig) {
      cacheConfig.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
      ;
   }

   /**
    * Copied from IfspnReplicationBuffer constructor.
    *
    * @return
    */
   public static EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder();
      globalConfig.defaultCacheName("default");
      globalConfig.serialization().marshaller(new JavaSerializationMarshaller())
            .allowList()
            .addRegexps(".*");

      var cacheManager = new DefaultCacheManager(globalConfig.build());

      return cacheManager;
   }

   private static Cache<String, Serializable> createReplicatorCache(EmbeddedCacheManager cacheManager) {
      cacheManager.defineConfiguration("replicator", initializeCacheConfig().build());
      var cache = cacheManager.<String, Serializable>getCache("replicator");
      cache.clear();
      return cache;
   }

   @Test
   void putRequestReplicaContainersInCache() throws Exception {
      try (EmbeddedCacheManager cacheManager = createCacheManager()) {
         LOG.info("Infinispan version: {}", cacheManager.getCacheManagerInfo().getVersion());
         Cache<String, Serializable> cache = createReplicatorCache(cacheManager);
         for (int i = 0; i < MAX_PAGES; i++) {
            final int fi = i;
            Stream.of("pageCapacity", "pageQueueID", "pageFull", "pageState")
                  .map(s -> KEY_PREFIX + ":" + fi + ":" + s)
                  .forEach(s -> cache.put(s, ""));
         }

         // Using synchronized access is crude but working here.
         var keys = new ArrayList<>(INITIAL_SIZE * N_THREADS);

         Runnable cachePutter = () -> {
            var value = createCachedEntity(COUNTER.incrementAndGet());
            var key = jobKey((Long) value.entityIndex());

            cache.put(key, value);
            synchronized (keys) {
               keys.add(key);
            }
         };

         Runnable cacheRemover = () -> {
            final Object randomKey;
            synchronized (keys) {
               var index = random.nextInt(keys.size());
               randomKey = keys.get(index);
               keys.remove(index);
            }
            cache.remove(randomKey);
         };

         // ===============================================================================================
         LOG.info("Initializing cache, inserting " + INITIAL_SIZE + " items");
         for (int i = 0; i < INITIAL_SIZE; i++) {
            cachePutter.run();
         }

         // ===============================================================================================
         LOG.info("Running random inserts and removals");
         var executors = Executors.newFixedThreadPool(N_THREADS);
         COUNTER.set(0);
         var watchedException = new AtomicReference<Exception>();
         Runnable randomOp = () -> {
            try {
               while (watchedException.get() == null && COUNTER.get() < MAXIMUM_ATTEMPTS) {
                  (random.nextBoolean() ? cachePutter : cacheRemover).run();
               }
            } catch (Exception e) {
                watchedException.set(e);
               throw e;
            }
         };
         for (int i = 0; i < N_THREADS; i++) {
            executors.submit(randomOp);
         }

         // ===============================================================================================
         LOG.info("Shutting down executors");
         executors.shutdown();
         executors.awaitTermination(TEST_MAX_TIME_SECONDS, TimeUnit.SECONDS);

         if (watchedException.get() != null) {
            throw watchedException.get();
         }
      } catch (Exception e) {
         if (isWatchedException(e)) {
            LOG.error("Watched exception observed: ", e);
         }
         throw e;
      }
   }

   private static boolean isWatchedException(Throwable e) {
      if (e == null) {
         return false;
      }
      if (e.getMessage().contains("Error reading header from")) {
         return true;
      }
      return isWatchedException(e.getCause()) || Stream.of(e.getSuppressed()).anyMatch(SoftIndexFileStoreStressTest2::isWatchedException);
   }

   private IfspnCachedEntity createCachedEntity(final int i) {
      return new IfspnCachedEntity(
            KEY_PREFIX + ":" + jobPage(i),
            (long) jobPage(i),
            jobId(i),
            new DummyReplica(jobId(i))
      );
   }

   private static long jobId(long i) {
      return 1000000000 + i;
   }

   private static int jobPage(long i) {
      return (int) (i % MAX_PAGES);
   }

   private static String jobKey(long i) {
      return KEY_PREFIX + ":" + jobPage(i) + ":entity:" + jobId(i);
   }

   /**
    * Copied from com.ysoft.cache.replicator.ifspn.IfspnReplicationSegment.DummyReplica
    */
   private record DummyReplica(Long replicationId) implements Serializable {
      @Serial
      private static final long serialVersionUID = 1487575110713603053L;
   }

   private record IfspnCachedEntity(String absolutePath, Long pageIndex, Object entityIndex, Serializable entityData) implements Serializable {
      @Serial
      private static final long serialVersionUID = 7891238912386310856L;
   }
}
