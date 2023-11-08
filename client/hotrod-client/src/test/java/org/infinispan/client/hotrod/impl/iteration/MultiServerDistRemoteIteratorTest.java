package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.client.hotrod.impl.iteration.Util.populateCache;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.test.TestingUtil;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

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

      final IntSet finished;

      public TestSegmentKeyTracker(int size) {
         this.finished = IntSets.concurrentSet(size);
      }

      @Override
      public boolean track(byte[] key, short status, ClassAllowList allowList) {
         return true;
      }

      @Override
      public void segmentsFinished(IntSet finishedSegments) {
         finished.addAll(finishedSegments);
      }

      @Override
      public Set<Integer> missedSegments() {
         return null;
      }
   }

   public void testSegmentFinishedCallback() {
      RemoteCache<Integer, AccountHS> cache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, Util::newAccount, cache);
      TestSegmentKeyTracker testSegmentKeyTracker = new TestSegmentKeyTracker(60);

      Publisher<Map.Entry<Integer, AccountHS>> publisher = cache.publishEntries(null, null, null, 3);
      TestingUtil.replaceField(testSegmentKeyTracker, "segmentKeyTracker", publisher, RemotePublisher.class);
      try (CloseableIterator<Map.Entry<Integer, AccountHS>> iterator = Closeables.iterator(publisher, 3)) {
         while (iterator.hasNext()) iterator.next();
         assertEquals(60, testSegmentKeyTracker.finished.size());
      }
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer()
            .host("localhost")
            .port(serverPort)
            .maxRetries(maxRetries())
            .balancingStrategy(() -> new PreferredServerBalancingStrategy(new InetSocketAddress("localhost", serverPort)));
      return clientBuilder;
   }

   @Test
   public void testIterationRouting() throws Exception {
      for (int i = 0; i < clients.size(); i++) {
         int clientOffset = i;
         RemoteCacheManager client = client(i);
         KeyTracker segmentKeyTracker = Mockito.mock(KeyTracker.class);
         Mockito.when(segmentKeyTracker.track(Mockito.any(), Mockito.anyShort(), Mockito.any()))
               .then(invocation -> {
                  assertIterationActiveOnlyOnServer(clientOffset);
                  return invocation.callRealMethod();
               });
         Publisher<Map.Entry<Object, Object>> publisher = client.getCache().publishEntries(null, null, null, 10);
         TestingUtil.replaceField(segmentKeyTracker, "segmentKeyTracker", publisher, RemotePublisher.class);

         Flowable.fromPublisher(publisher)
               .lastStage(null)
               .toCompletableFuture()
               .get(10, TimeUnit.SECONDS);
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
