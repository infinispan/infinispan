package org.infinispan.client.hotrod.impl.iteration;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.MultiServerDistRemoteIteratorTest")
public class MultiServerDistRemoteIteratorTest extends BaseMultiServerRemoteIteratorTest {

   private static final int NUM_SERVERS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return builder;
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer()
              .host("localhost")
              .port(serverPort)
              .maxRetries(maxRetries())
              .balancingStrategy(new PreferredServerBalancingStrategy(new InetSocketAddress("localhost", serverPort)))
              .pingOnStartup(false);
      return clientBuilder;
   }

   @Test
   public void testIterationRouting() throws Exception {
      for (int i = 0; i < clients.size(); i++) {
         RemoteCacheManager client = client(i);
         try (CloseableIterator<Map.Entry<Object, Object>> ignored = client.getCache().retrieveEntries(null, 10)) {
            assertIterationActiveOnlyOnServer(i);
         }
      }
   }

   private void assertIterationActiveOnlyOnServer(int index) {
      for (int i = 0; i < servers.size(); i++) {
         int activeIterations = server(i).iterationManager().activeIterations();
         if (i == index) {
            assertEquals(1L, activeIterations);
         } else {
            assertEquals(0L, activeIterations);
         }
      }
   }

}
