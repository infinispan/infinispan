package org.infinispan.server.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
@org.infinispan.server.test.core.tags.Database
public class PooledConnectionOperations {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = PersistenceIT.SERVERS;

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testTwoCachesSameCacheStore(Database database) {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.DIST_SYNC, database, false, false);
      RemoteCache<String, String> cache1 = SERVERS.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).withQualifier("1").create();
      RemoteCache<String, String> cache2 = SERVERS.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).withQualifier("2").create();

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
      RemoteCache<String, String> cache = SERVERS.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();

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

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testPreload(Database database) throws Exception {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
              .setLockingConfigurations();
      RemoteCache<String, String> cache = SERVERS.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
      cache.put("k1", "v1");
      cache.put("k2", "v2");

      SERVERS.getServerDriver().stop(0);
      SERVERS.getServerDriver().restart(0);

      // test preload==true, entries should be immediately in the cache after restart
      assertEquals(2, cache.withFlags(Flag.SKIP_CACHE_LOAD).size());
      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));
      cache.clear();
   }

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testSoftRestartWithPassivation(Database database) throws Exception {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, true, false)
              .setEviction()
              .setLockingConfigurations();
      RemoteCache<String, String> cache = SERVERS.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      cache.put("k3", "v3");

      //now k3 is evicted and stored in store
      assertEquals(2, cache.withFlags(Flag.SKIP_CACHE_LOAD).size());

     SERVERS.getServerDriver().stop(0);
     SERVERS.getServerDriver().restart(0); //soft stop should store all entries from cache to store

     // test preload==false
     assertEquals(0, cache.withFlags(Flag.SKIP_CACHE_LOAD).size());
     // test purge==false, entries should remain in the database after restart
     assertEquals(3, cache.size());
     assertEquals("v1", cache.get("k1"));
      assertCleanCacheAndStore(cache);
   }

   protected void assertCleanCacheAndStore(RemoteCache cache) {
      cache.clear();
      assertEquals(0, cache.size());
   }

}
