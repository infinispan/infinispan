package org.infinispan.server.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
   public void testPreload(Database database) throws Exception {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
              .setLockingConfigurations();
      RemoteCache<String, String> cache = SERVERS.hotrod()
              .withClientConfiguration(database.getHotrodClientProperties())
              .withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
      cache.put("k1", "v1");
      cache.put("k2", "v2");

      SERVERS.getServerDriver().stopCluster();
      SERVERS.getServerDriver().restartCluster();

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
      RemoteCache<String, String> cache = SERVERS.hotrod()
              .withClientConfiguration(database.getHotrodClientProperties())
              .withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      cache.put("k3", "v3");

      //now k3 is evicted and stored in store
      assertEquals(2, cache.withFlags(Flag.SKIP_CACHE_LOAD).size());

     SERVERS.getServerDriver().stopCluster();
     SERVERS.getServerDriver().restartCluster(); //soft stop should store all entries from cache to store

     // test preload==false
     assertEquals(0, cache.withFlags(Flag.SKIP_CACHE_LOAD).size());
     // test purge==false, entries should remain in the database after restart
     assertEquals(3, cache.size());
     assertEquals("v1", cache.get("k1"));
      assertCleanCacheAndStore(cache);
   }

   static void assertCleanCacheAndStore(RemoteCache<?, ?> cache) {
      cache.clear();
      assertEquals(0, cache.size());
   }

}
