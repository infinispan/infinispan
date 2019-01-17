package org.infinispan.all.embedded;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Self standing functional tests for infinispan-embedded UberJar.
 *
 * @author Tomas Sykora (tsykora@redhat.com)
 */
public class EmbeddedAllTest {

   private static final Log log = LogFactory.getLog(EmbeddedAllTest.class);
   private static EmbeddedCacheManager manager;
   private static EmbeddedCacheManager manager2;

   @BeforeClass
   public static void beforeTest() throws Exception {

      GlobalConfiguration globalConfiguration = GlobalConfigurationBuilder
            .defaultClusteredBuilder()
            .transport().nodeName("node1")
            .build();

      GlobalConfiguration globalConfiguration2 = GlobalConfigurationBuilder
            .defaultClusteredBuilder()
            .transport().nodeName("node2")
            .build();

      manager = new DefaultCacheManager(globalConfiguration);
      manager2 = new DefaultCacheManager(globalConfiguration2);
   }

   @AfterClass
   public static void cleanUp() {
      killCacheManagers(true, manager, manager2);
   }

   @Test
   public void testAllEmbeddedClustered() {

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().fetchInMemoryState(true)
            .clustering().hash().numOwners(2);

      manager.defineConfiguration("distributed-cache", builder.build());
      manager2.defineConfiguration("distributed-cache", builder.build());

      Cache<Object, Object> cache = manager.getCache("distributed-cache");
      Cache<Object, Object> cache2 = manager2.getCache("distributed-cache");

      cache.put("key1", "value1");
      assertEquals("value1", cache.get("key1"));

      // distributed?
      cache2.put("key2", "value2");
      assertEquals("value2", cache2.get("key2"));
      assertEquals("value1", cache2.get("key1"));
   }

   @Test
   public void testAllEmbeddedJpaStore() {

      ConfigurationBuilder builderJpaLocalCache = new ConfigurationBuilder();
      builderJpaLocalCache.clustering().cacheMode(CacheMode.LOCAL)
            .persistence().passivation(true)
            .addStore(JpaStoreConfigurationBuilder.class)
            .persistenceUnitName("org.infinispan.persistence.jpa")
            .entityClass(KeyValueEntity.class)
            .purgeOnStartup(false).preload(true);

      manager.defineConfiguration("jpa-local-cache", builderJpaLocalCache.build());
      Cache<Object, Object> jpaLocalCache = manager.getCache("jpa-local-cache");

      KeyValueEntity kve1 = new KeyValueEntity("kve_key1", "kve_value1");
      KeyValueEntity kve2 = new KeyValueEntity("kve_key2", "kve_value2");

      jpaLocalCache.put("kve_key1", kve1);
      jpaLocalCache.put("kve_key2", kve2);

      assertEquals(kve1, jpaLocalCache.get("kve_key1"));
      assertEquals(kve2, jpaLocalCache.get("kve_key2"));

      jpaLocalCache.stop();
      jpaLocalCache.start();

      // survived?
      KeyValueEntity result1 = (KeyValueEntity) jpaLocalCache.get("kve_key1");
      assertEquals(kve1.getValue(), result1.getValue());
      assertEquals(kve1.getK(), result1.getK());
      KeyValueEntity result2 = (KeyValueEntity) jpaLocalCache.get("kve_key2");
      assertEquals(kve2.getValue(), result2.getValue());
      assertEquals(kve2.getK(), result2.getK());
   }

   @Test
   public void testAllEmbeddedFileStore() {

      ConfigurationBuilder builderFcsLocalCache = new ConfigurationBuilder();
      builderFcsLocalCache.clustering().cacheMode(CacheMode.LOCAL)
            .persistence().passivation(true)
            .addSingleFileStore().location("/tmp/").purgeOnStartup(false);

      manager.defineConfiguration("fcs-local-cache", builderFcsLocalCache.build());
      Cache<Object, Object> fcsLocalCache = manager.getCache("fcs-local-cache");

      testDataSurvived(fcsLocalCache);
   }

   @Test
   public void testAllEmbeddedJdbcStore() {

      ConfigurationBuilder builderJdbcLocalCache = new ConfigurationBuilder();
      builderJdbcLocalCache.clustering().cacheMode(CacheMode.LOCAL)
            .persistence().passivation(true)
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .purgeOnStartup(false).preload(true)
            .table()
            .dropOnExit(false).createOnStart(true)
            .tableNamePrefix("ISPN_STRING_TABLE")
            .idColumnName("ID_COLUMN").idColumnType("VARCHAR(255)")
            .dataColumnName("DATA_COLUMN").dataColumnType("BINARY")
            .timestampColumnName("TIMESTAMP_COLUMN").timestampColumnType("BIGINT")
            .connectionPool().connectionUrl("jdbc:h2:mem:infinispan_binary_based;DB_CLOSE_DELAY=-1")
            .username("sa").driverClass("org.h2.Driver");

      manager.defineConfiguration("jdbc-local-cache", builderJdbcLocalCache.build());
      Cache<Object, Object> jdbcLocalCache = manager.getCache("jdbc-local-cache");

      testDataSurvived(jdbcLocalCache);
   }

   @Test
   public void testAllEmbeddedRocksDbStore() {

      ConfigurationBuilder builderRocksDbLocalCache = new ConfigurationBuilder();
      builderRocksDbLocalCache.clustering().cacheMode(CacheMode.LOCAL)
            .persistence().passivation(true)
            .addStore(RocksDBStoreConfigurationBuilder.class)
            .location("/tmp/rocksdb/data")
            .expiredLocation("/tmp/rocksdb/expired").build();

      manager.defineConfiguration("rocksdb-local-cache", builderRocksDbLocalCache.build());
      Cache<Object, Object> rocksDbLocalCache = manager.getCache("rocksdb-local-cache");

      testDataSurvived(rocksDbLocalCache);
   }

   @Test
   public void testEmbeddedClusterExec() throws Exception {
      ClusterExecutor clusterExecutor = manager.executor();
      List<String> synchronizedList = Collections.synchronizedList(new ArrayList<>());
      CompletableFuture<Void> future = clusterExecutor.submitConsumer(new TestCallable(), (a, v, t) -> {
         if (t != null) {
            throw new CacheException(t);
         }
         synchronizedList.add(v);
      });
      future.join();
      for(String result : synchronizedList) {
         assertEquals("OK", result);
      }
   }

   static final class TestCallable implements Function<EmbeddedCacheManager, String>, Serializable {
      @Override
      public String apply(EmbeddedCacheManager embeddedCacheManager) {
         return "OK";
      }
   }

   private void testDataSurvived(Cache<Object, Object> cache) {
      String key1 = "key1_" + cache.getName();
      String key2 = "key2_" + cache.getName();
      String value1 = "value1_" + cache.getName();
      String value2 = "value2_" + cache.getName();

      cache.put(key1, value1);
      cache.put(key2, value2);

      assertEquals(value1, cache.get(key1));
      assertEquals(value2, cache.get(key2));

      cache.stop();
      cache.start();

      // survived?
      assertEquals(value1, cache.get(key1));
      assertEquals(value2, cache.get(key2));
   }

   private static void killCacheManagers(boolean clear, EmbeddedCacheManager... cacheManagers) {
      // stop the caches first so that stopping the cache managers doesn't trigger a rehash
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            killCaches(clear, getRunningCaches(cm));
         } catch (Throwable e) {
            log.warn("Problems stopping cache manager " + cm, e);
         }
      }
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            if (cm != null) cm.stop();
         } catch (Throwable e) {
            log.warn("Problems killing cache manager " + cm, e);
         }
      }
   }

   /**
    * Kills a cache - stops it and rolls back any associated txs
    */
   private static void killCaches(boolean clear, Collection<Cache> caches) {
      for (Cache c : caches) {
         try {
            if (c != null && c.getStatus() == ComponentStatus.RUNNING) {
               TransactionManager tm = c.getAdvancedCache().getTransactionManager();
               if (tm != null) {
                  try {
                     tm.rollback();
                  }
                  catch (Exception e) {
                     // don't care
                  }
               }
               if (c.getAdvancedCache().getRpcManager() != null) {
                  log.tracef("Cache contents on %s before stopping: %s", c.getAdvancedCache().getRpcManager().getAddress(), c.entrySet());
               } else {
                  log.tracef("Cache contents before stopping: %s", c.entrySet());
               }
               if (clear) {
                  try {
                     c.clear();
                  } catch (Exception ignored) {}
               }
               c.stop();
            }
         }
         catch (Throwable t) {
            log.tracef("Problems with killing caches: %s", t.getStackTrace());
         }
      }
   }

   private static Set<Cache> getRunningCaches(EmbeddedCacheManager cacheContainer) {
      Set<Cache> running = new HashSet<Cache>();
      if (cacheContainer == null || !cacheContainer.getStatus().allowInvocations())
         return running;

      for (String cacheName : cacheContainer.getCacheNames()) {
         if (cacheContainer.isRunning(cacheName)) {
            Cache c = cacheContainer.getCache(cacheName);
            if (c.getStatus().allowInvocations()) running.add(c);
         }
      }

      if (cacheContainer.isDefaultRunning()) {
         Cache defaultCache = cacheContainer.getCache();
         if (defaultCache.getStatus().allowInvocations()) running.add(defaultCache);
      }

      return running;
   }
}
