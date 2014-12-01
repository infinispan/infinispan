package org.infinispan.all.embedded;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.all.embedded.util.EmbeddedUtils;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
            .defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
            .transport().nodeName("node1")
            .build();

      GlobalConfiguration globalConfiguration2 = GlobalConfigurationBuilder
            .defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
            .transport().nodeName("node2")
            .build();

      manager = new DefaultCacheManager(globalConfiguration);
      manager2 = new DefaultCacheManager(globalConfiguration2);
   }

   @AfterClass
   public static void cleanUp() {
      EmbeddedUtils.killCacheManagers(true, manager, manager2);
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
   public void testAllEmbeddedLevelDbStore() {

      ConfigurationBuilder builderLevelDbLocalCache = new ConfigurationBuilder();
      builderLevelDbLocalCache.clustering().cacheMode(CacheMode.LOCAL)
            .persistence().passivation(true)
            .addStore(LevelDBStoreConfigurationBuilder.class)
            .location("/tmp/leveldb/data")
            .expiredLocation("/tmp/leveldb/expired").build();

      manager.defineConfiguration("leveldb-local-cache", builderLevelDbLocalCache.build());
      Cache<Object, Object> levelDbLocalCache = manager.getCache("leveldb-local-cache");

      testDataSurvived(levelDbLocalCache);
   }

   @Test
   public void testEmbeddedDistExec() throws Exception {
      DefaultExecutorService defaultExecutorService = new DefaultExecutorService(manager.getCache());
      List<Future<String>> results = defaultExecutorService.submitEverywhere(new TestCallable());
      for(Future<String> result : results) {
         assertEquals("OK", result.get());
      }
   }

   static final class TestCallable implements Callable<String>, Serializable {
      public String call() throws Exception {
         return "OK";
      }
   }

   @Test
   public void testEmbeddedMapReduce() throws Exception {
      Cache<String, String> cache = manager.getCache();
      cache.put("k1", "v1");
      cache.put("k2", "v1");
      MapReduceTask<String, String, String, Integer> task = new MapReduceTask<String, String, String, Integer>(cache);
      task.mappedWith(new TestMapper()).reducedWith(new TestReducer());
      Map<String, Integer> result = task.execute();
      assertEquals(1, result.size());
      assertEquals(2, result.get("v1").intValue());
   }

   static class TestMapper implements Mapper<String, String, String, Integer> {
      public void map(String key, String value, Collector<String, Integer> collector) {
         collector.emit(value, 1);
      }
   }

   static class TestReducer implements Reducer<String, Integer> {

      public Integer reduce(String reducedKey, Iterator<Integer> iter) {
         int count = 0;
         for(; iter.hasNext(); iter.next()) {
            count++;
         }
         return count;
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
}
