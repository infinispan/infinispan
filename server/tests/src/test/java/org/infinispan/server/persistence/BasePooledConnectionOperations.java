package org.infinispan.server.persistence;

import static org.infinispan.server.persistence.PooledConnectionOperations.assertCleanCacheAndStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@org.infinispan.server.test.core.tags.Database
public class BasePooledConnectionOperations {

   @InfinispanServer(PersistenceIT.class)
   public static TestClientDriver SERVERS;

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testTwoCachesSameCacheStore(Database database) {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.DIST_SYNC, database, false, false);
      RemoteCache<String, String> cache1 = SERVERS.hotrod()
            .withClientConfiguration(database.getHotrodClientProperties())
            .withServerConfiguration(jdbcUtil.getConfigurationBuilder()).withQualifier("1").create();
      RemoteCache<String, String> cache2 = SERVERS.hotrod()
            .withClientConfiguration(database.getHotrodClientProperties())
            .withServerConfiguration(jdbcUtil.getConfigurationBuilder()).withQualifier("2").create();

      cache1.put("k1", "v1");
      String firstK1 = cache1.get("k1");
      assertEquals("v1", firstK1);
      assertNull(cache2.get("k1"));

      cache2.put("k2", "v2");
      assertEquals("v2", cache2.get("k2"));
      assertNull(cache1.get("k2"));

      assertCleanCacheAndStore(cache1);
      assertCleanCacheAndStore(cache2);
   }

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testPutGetRemove(Database database) {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.DIST_SYNC, database, false, false);
      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withClientConfiguration(database.getHotrodClientProperties())
            .withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();

      cache.put("k1", "v1");
      cache.put("k2", "v2");

      assertNotNull(cache.get("k1"));
      assertNotNull(cache.get("k2"));

      cache.stop();
      cache.start();

      assertNotNull(cache.get("k1"));
      assertNotNull(cache.get("k2"));
      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));
      cache.remove("k1");
      assertNull(cache.get("k1"));
      assertCleanCacheAndStore(cache);
   }
}
