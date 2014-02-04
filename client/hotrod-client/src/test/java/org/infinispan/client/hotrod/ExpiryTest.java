package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

/**
 * This test verifies that an entry can be expired from the Hot Rod server
 * using the default expiry lifespan or maxIdle. </p>
 *
 * This test is disabled because the limitations of the protocol do not allow
 * for this to work as expected. This test will be enabled once v2 of the
 * protocol has been implemented and the functionality is there to support it.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "unstable", testName = "client.hotrod.ExpiryTest", description = "original group: functional")
public class ExpiryTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.expiration().lifespan(2000L).maxIdle(3000L);
      createHotRodServers(1, builder);
   }

   public void testGlobalExpiry(Method m) throws Exception {
      RemoteCacheManager client0 = client(0);
      RemoteCache<Object, Object> cache0 = client0.getCache();
      String v1 = v(m);
      cache0.put(1, v1);
      Thread.sleep(2500);
      assertEquals(null, cache0.get(1));
   }

}
