package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.*;
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

   public static final int EXPIRATION_TIMEOUT = 3000;
   public static final int EVICTION_CHECK_TIMEOUT = 2000;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.expiration().lifespan(EXPIRATION_TIMEOUT);
      createHotRodServers(1, builder);
   }

   public void testGlobalExpiry(Method m) throws Exception {
      RemoteCacheManager client0 = client(0);
      RemoteCache<Integer, String> cache0 = client0.getCache();
      String v1 = v(m);
      cache0.put(1, v1);
      expectCachedThenExpired(1, cache0);
   }

   private void expectCachedThenExpired(Integer key, RemoteCache<Integer, String> cache) {
      final long startTime = now();
      final long expiration = EXPIRATION_TIMEOUT;
      sleepThread(expiration + EVICTION_CHECK_TIMEOUT);

      // Make sure that in the next X secs data is removed
      while (!moreThanDurationElapsed(startTime, expiration + EVICTION_CHECK_TIMEOUT)) {
         if (cache.get(key) == null) return;
      }

      assertNull(cache.get(key));
   }

}
