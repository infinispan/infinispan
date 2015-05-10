package org.infinispan.client.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import static org.testng.AssertJUnit.assertFalse;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(testName = "client.hotrod.MillisecondExpirationTest", groups = "functional")
public class MillisecondExpirationTest extends DefaultExpirationTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @Test
   public void testDefaultExpiration() throws Exception {
      remoteCache.put("Key", "Value", 50, TimeUnit.MILLISECONDS);
      Thread.sleep(100);
      assertFalse(remoteCache.containsKey("Key"));
   }
}
