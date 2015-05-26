package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

@Test(groups = "functional", testName = "client.hotrod.retry.CompleteShutdownReplRetryTest")
public class CompleteShutdownReplRetryTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      // Empty
   }

   @Override
   protected int maxRetries() {
      return 1;
   }

   public void testRetryAfterCompleteShutdown() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      createHotRodServers(3, builder);
      try {
         int initialServerPort = server(0).getPort();

         assertClusterSize("Cluster should be formed", 3);

         RemoteCache<Integer, String> client = client(0).getCache();
         client.put(1, "one");
         assertEquals("one", client.get(1));

         killServer(0);
         assertEquals("one", client.get(1));
         killServer(0);
         assertEquals("one", client.get(1));
         killServer(0);
         try {
            assertEquals("one", client.get(1));
            fail("Should have thrown exception");
         } catch (TransportException e) {
            // Ignore, expected
         }

         addHotRodServer(builder, initialServerPort);
         client.put(1, "one");
         assertEquals("one", client.get(1));
      } finally {
         destroy();
      }
   }

}
