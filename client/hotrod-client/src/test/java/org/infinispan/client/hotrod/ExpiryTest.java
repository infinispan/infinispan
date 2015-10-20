package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * This test verifies that an entry can be expired from the Hot Rod server
 * using the default expiry lifespan or maxIdle. </p>
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
      timeService = new ControlledTimeService(0);
      TestingUtil.replaceComponent(cacheManagers.get(0), TimeService.class, timeService, true);
   }

   public void testGlobalExpiryPut() {
      expectExpiryAfterRequest(Req.PUT);
   }

   public void testGlobalExpiryPutWithFlag() {
      expectExpiryAfterRequest(Req.PUT, client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING));
   }

   public void testGlobalExpiryPutAll() {
      expectExpiryAfterRequest(Req.PUT_ALL);
   }

   public void testGlobalExpiryPutAllWithFlag() {
      expectExpiryAfterRequest(Req.PUT_ALL, client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING));
   }

   public void testGlobalExpiryPutIfAbsent() {
      expectExpiryAfterRequest(Req.PUT_IF_ABSENT);
   }

   public void testGlobalExpiryPutIfAbsentWithFlag() {
      expectExpiryAfterRequest(Req.PUT_IF_ABSENT, client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING));
   }

   public void testGlobalExpiryReplace() {
      client(0).getCache().put(1, "v0");
      expectExpiryAfterRequest(Req.REPLACE);
   }

   public void testGlobalExpiryReplaceFlag() {
      client(0).getCache().put(1, "v0");
      expectExpiryAfterRequest(Req.REPLACE, client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING));
   }

   public void testGlobalExpiryReplaceWithVersion() {
      client(0).getCache().put(1, "v0");
      long version = client(0).getCache().getVersioned(1).getVersion();
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      Req.REPLACE_WITH_VERSION.execute(cache0, version);
      expectCachedThenExpired(cache0);
   }

   public void testGlobalExpiryReplaceWithVersionFlag() {
      client(0).getCache().put(1, "v0");
      long version = client(0).getCache().getVersioned(1).getVersion();
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      Req.REPLACE_WITH_VERSION.execute(client(0).<Integer, String>getCache().withFlags(Flag.SKIP_INDEXING), version);
      expectCachedThenExpired(cache0);
   }

   private void expectExpiryAfterRequest(Req req) {
      RemoteCache<Integer, String> cache0 = client(0).getCache();
      req.execute(cache0);
      expectCachedThenExpired(cache0);
   }

   private void expectExpiryAfterRequest(Req req, RemoteCache<Integer, String> cache0) {
      req.execute(cache0);
      expectCachedThenExpired(cache0);
   }

   private void expectCachedThenExpired(RemoteCache<Integer, String> cache) {
      assertEquals("v1", cache.get(1));
      timeService.advance(EXPIRATION_TIMEOUT + 100);
      assertNull(cache.get(1));
   }

   private enum Req {
      PUT {
         @Override
         void execute(RemoteCache<Integer, String> c) {
            c.put(1, "v1");
         }
      },

      PUT_IF_ABSENT {
         @Override
         void execute(RemoteCache<Integer, String> c) {
            c.putIfAbsent(1, "v1");
         }
      },

      PUT_ALL {
         @Override
         void execute(RemoteCache<Integer, String> c) {
            Map<Integer, String> data = new HashMap<Integer, String>();
            data.put(1, "v1");
            c.putAll(data);
         }
      },

      REPLACE {
         @Override
         void execute(RemoteCache<Integer, String> c) {
            c.replace(1, "v1");
         }
      },

      REPLACE_WITH_VERSION {
         @Override
         void execute(RemoteCache<Integer, String> c, Long version) {
            c.replaceWithVersion(1, "v1", version);
         }
      },
      ;

      void execute(RemoteCache<Integer, String> c) {
         execute(c, null);
      }

      void execute(RemoteCache<Integer, String> c, Long version) {
         execute(c);
      }
   }

}
