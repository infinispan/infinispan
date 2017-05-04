package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.OperationStatus.ParseError;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;

import java.lang.reflect.Method;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests behaviour of Hot Rod servers with asymmetric clusters
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodAsymmetricClusterTest")
public class HotRodAsymmetricClusterTest extends HotRodMultiNodeTest {

  protected String cacheName() { return "asymmetricCache"; }

  protected ConfigurationBuilder createCacheConfig() {
     return hotRodCacheConfiguration(
           getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
  }


  @Override
  protected void createCacheManagers() {
     for (int i = 0; i < 2; i++) {
        EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration());
        cacheManagers.add(cm);
        if (i == 0) {
           cm.defineConfiguration(cacheName(), createCacheConfig().build());
        }
     }
  }

   public void testPutInCacheDefinedNode(Method m) {
      TestResponse resp = clients().get(0).put(k(m) , 0, 0, v(m));
      assertStatus(resp, Success);
   }

   public void testPutInNonCacheDefinedNode(Method m) {
      TestResponse resp = clients().get(1).put(k(m) , 0, 0, v(m));
      assertStatus(resp, ParseError);
   }

}
