package org.infinispan.all.embedded;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.testng.annotations.*;

import java.io.IOException;

import static org.testng.AssertJUnit.assertEquals;

@Test
public class EmbeddedAllTest {

   private EmbeddedCacheManager manager;
   private EmbeddedCacheManager manager2;

   public void testAllEmbeddedClustered() throws IOException {

      System.setProperty("java.net.preferIPv4Stack", "true");
      System.setProperty("jgroups.bind_addr", "127.0.0.1");

      GlobalConfiguration globalConfiguration = GlobalConfigurationBuilder
            .defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
            .transport().nodeName("node1").addProperty("configurationFile", "jgroups.xml")
            .build();

      GlobalConfiguration globalConfiguration2 = GlobalConfigurationBuilder
            .defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
            .transport().nodeName("node2").addProperty("configurationFile", "jgroups.xml")
            .build();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().fetchInMemoryState(true)
            .clustering().hash().numOwners(2);

      manager = new DefaultCacheManager(globalConfiguration);
      manager.defineConfiguration("distributed-cache", builder.build());

      manager2 = new DefaultCacheManager(globalConfiguration2);
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

   public void testAllEmbeddedFileStore() {

      ConfigurationBuilder builderFcsLocalCache = new ConfigurationBuilder();
      builderFcsLocalCache.clustering().cacheMode(CacheMode.LOCAL)
            .persistence().passivation(true)
            .addSingleFileStore().location("/tmp/").purgeOnStartup(false);

      manager.defineConfiguration("fcs-local-cache", builderFcsLocalCache.build());
      Cache<Object, Object> fcsLocalCache = manager.getCache("fcs-local-cache");

      testDataSurvived(fcsLocalCache);
   }

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

   private void testDataSurvived(Cache cache) {
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

   @AfterClass
   private void cleanUp() {
      manager.stop();
      manager2.stop();
   }
}
