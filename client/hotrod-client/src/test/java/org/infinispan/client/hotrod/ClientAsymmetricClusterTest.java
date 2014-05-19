package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests behaviour of Hot Rod clients with asymmetric clusters.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "client.hotrod.ClientAsymmetricClusterTest")
public class ClientAsymmetricClusterTest extends MultiHotRodServersTest {

   private static final String CACHE_NAME = "asymmetricCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));

      createHotRodServers(2, builder);

      // Define replicated cache in only one of the nodes
      manager(0).defineConfiguration(CACHE_NAME, builder.build());
   }

   @Test(expectedExceptions = HotRodClientException.class,
         expectedExceptionsMessageRegExp = ".*CacheNotFoundException.*")
   public void testAsymmetricCluster() {
      RemoteCacheManager client0 = client(0);
      RemoteCache<Object, Object> cache0 = client0.getCache(CACHE_NAME);
      cache0.put(1, "v1");
      assertEquals("v1", cache0.get(1));
      cache0.put(2, "v1");
      assertEquals("v1", cache0.get(2));
      cache0.put(3, "v1");
      assertEquals("v1", cache0.get(3));
   }

}
