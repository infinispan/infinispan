package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.testng.annotations.Test;

/**
 * This test verifies that an entry can be expired from the Hot Rod server
 * using the default expiry lifespan or maxIdle.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "client.hotrod.ExpiryTest")
public class ExpiryTest extends MultiHotRodServersTest {

   public static final int EXPIRATION_TIMEOUT = 6000;

   private ControlledTimeService timeService;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      builder.expiration().lifespan(EXPIRATION_TIMEOUT);
      createHotRodServers(1, builder);
      timeService = new ControlledTimeService();
      TestingUtil.replaceComponent(cacheManagers.get(0), TimeService.class, timeService, true);
   }

   public void testGlobalExpiryPut() {
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      Req.PUT.execute(cache0,0,"v0");
      expectCachedThenExpired(cache0, 0, "v0");
   }

   public void testGlobalExpiryPutWithFlag() {
      RemoteCache<Integer, String> cache0 = client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING);
      Req.PUT.execute(cache0,1,"v0");
      expectCachedThenExpired(cache0, 1, "v0");
   }

   public void testGlobalExpiryPutAll() {
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      Map<Integer, String> data = new HashMap<>();
      data.put(2,"v0");
      Req.PUT_ALL.execute(cache0,data);
      expectCachedThenExpired(cache0, 2, "v0");
   }

   public void testGlobalExpiryPutAllWithFlag() {
      RemoteCache<Integer, String> cache0 = client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING);
      Map<Integer, String> data = new HashMap<>();
      data.put(3, "v0");
      Req.PUT_ALL.execute(cache0,data);
      expectCachedThenExpired(cache0, 3, "v0");
   }

   public void testGlobalExpiryPutIfAbsent() {
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      Req.PUT_IF_ABSENT.execute(cache0, 4, "v0");
      expectCachedThenExpired(cache0, 4, "v0");
   }

   public void testGlobalExpiryPutIfAbsentWithFlag() {
      RemoteCache<Integer, String> cache0 = client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING);
      Req.PUT_IF_ABSENT.execute(cache0, 5, "v0");
      expectCachedThenExpired(cache0, 5, "v0");
   }

   public void testGlobalExpiryReplace() {
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      cache0.put(6,"v1");
      Req.REPLACE.execute(cache0, 6, "v0");
      expectCachedThenExpired(cache0, 6, "v0");

   }

   public void testGlobalExpiryReplaceFlag() {
      RemoteCache<Integer, String> cache0 = client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING);
      cache0.put(7,"v1");
      Req.REPLACE.execute(cache0, 7, "v0");
      expectCachedThenExpired(cache0, 7, "v0");
   }

   public void testGlobalExpiryReplaceWithVersion() {
      client(0).getCache().put(8, "v0");
      long version = client(0).getCache().getVersioned(8).getVersion();
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      Req.REPLACE_WITH_VERSION.execute(cache0, 8,"v1",version);
      expectCachedThenExpired(cache0, 8, "v1");
   }

   public void testGlobalExpiryReplaceWithVersionFlag() {
      client(0).getCache().put(9, "v0");
      long version = client(0).getCache().getVersioned(9).getVersion();
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      Req.REPLACE_WITH_VERSION.execute(client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING),9,"v1",version);
      expectCachedThenExpired(cache0,9,"v1");
   }


   private void expectCachedThenExpired(RemoteCache<Integer, String> cache, int key, String value) {
      assertEquals(value, cache.get(key));
      timeService.advance(EXPIRATION_TIMEOUT + 100);
      assertNull(cache.get(key));
   }

   private enum Req {
      PUT {
         @Override
         void execute(RemoteCache<Integer, String> c, int key ,String value) {
            c.put(key, value);
         }
      },

      PUT_IF_ABSENT {
         @Override
         void execute(RemoteCache<Integer, String> c, int key, String value) {
            c.putIfAbsent(key, value);
         }
      },

      PUT_ALL {
         @Override
         void execute(RemoteCache<Integer, String> c,Map<Integer, String> data) {
            c.putAll(data);
         }
      },

      REPLACE {
         @Override
         void execute(RemoteCache<Integer, String> c, int key, String value) {
            c.replace(key, value);
         }
      },

      REPLACE_WITH_VERSION {
         @Override
         void execute(RemoteCache<Integer, String> c, int key, String value, Long version) {
            c.replaceWithVersion(key, value, version);
         }
      },
      ;

      void execute(RemoteCache<Integer, String> c, int key, String value,  Long version) {
         execute(c,key,value,version);
      }

      void execute(RemoteCache<Integer, String> c, int key, String value) {
         execute(c,key,value);
      }

      void execute(RemoteCache<Integer, String> c, Map<Integer, String> data) {
         execute(c,data);
      }
   }

}
