package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

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
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numSegments(60).numOwners(2);
      return builder;
   }


   private static class TestSegmentKeyTracker implements KeyTracker {

      List<Integer> finished = new ArrayList<>();

      @Override
      public boolean track(byte[] key, short status, ClassWhiteList whitelist) {
         return true;
      }

      @Override
      public void segmentsFinished(byte[] finishedSegments) {
         BitSet bitSet = BitSet.valueOf(finishedSegments);
         bitSet.stream().forEach(finished::add);
      }

      @Override
      public Set<Integer> missedSegments() {
         return null;
      }
   }

   public void testSegmentFinishedCallback() {
      RemoteCache<Integer, AccountHS> cache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, this::newAccount, cache);
      TestSegmentKeyTracker testSegmentKeyTracker = new TestSegmentKeyTracker();

      try (CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, 3)) {
         TestingUtil.replaceField(testSegmentKeyTracker, "segmentKeyTracker", iterator, RemoteCloseableIterator.class);
         while (iterator.hasNext()) iterator.next();
         assertEquals(60, testSegmentKeyTracker.finished.size());
      }
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer()
            .host("localhost")
            .port(serverPort)
            .maxRetries(maxRetries())
            .balancingStrategy(new PreferredServerBalancingStrategy(new InetSocketAddress("localhost", serverPort)));
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
         int activeIterations = server(i).getIterationManager().activeIterations();
         if (i == index) {
            assertEquals(1L, activeIterations);
         } else {
            assertEquals(0L, activeIterations);
         }
      }
   }

}
