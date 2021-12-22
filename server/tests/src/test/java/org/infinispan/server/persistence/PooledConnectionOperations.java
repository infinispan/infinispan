package org.infinispan.server.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
@Category(Persistence.class)
@RunWith(Parameterized.class)
public class PooledConnectionOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = PersistenceIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final Database database;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      String[] databaseTypes = PersistenceIT.DATABASE_LISTENER.getDatabaseTypes();
      List<Object[]> params = new ArrayList<>(databaseTypes.length);
      for (String databaseType : databaseTypes) {
         params.add(new Object[]{databaseType});
      }
      return params;
   }

   public PooledConnectionOperations(String databaseType) {
      this.database = PersistenceIT.DATABASE_LISTENER.getDatabase(databaseType);
   }

   @Test
   public void testTwoCachesSameCacheStore() {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.DIST_SYNC, database, false, false);
      RemoteCache<String, String> cache1 = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).withQualifier("1").create();
      RemoteCache<String, String> cache2 = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).withQualifier("2").create();

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

   @Test
   public void testPutGetRemove() {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.DIST_SYNC, database, false, false);
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();

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

   @Test
   public void testPreload() throws Exception {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
              .setLockingConfigurations();
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
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

   @Test
   public void testSoftRestartWithPassivation() throws Exception {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, true, false)
              .setEvition()
              .setLockingConfigurations();
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
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
