package org.infinispan.client.hotrod.query.pressure;

import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.CHUNK_SIZE;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.chunk;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Sale;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.pressure.LargePutAllPressureTest")
public class LargePutAllPressureTest extends SingleHotRodServerTest {

   private final static int SIZE = 15_000;

   private final Random fixedSeedPseudoRandom = new Random(739);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
               .addIndexedEntity("Sale")
            .writer()
               .queueCount(1)
               .queueSize(10_000);

      return TestCacheManagerFactory.createServerModeCacheManager(config);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder
      createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            super.createHotRodClientConfigurationBuilder(host, serverPort);

      builder.socketTimeout(SIZE * 2);
      return builder;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Sale.SaleSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      int days = SIZE / CHUNK_SIZE;
      HashMap<String, Sale> bulkPut = new HashMap<>(SIZE);
      for (int day = 1; day <= days; day++) {
         bulkPut.putAll(chunk(day, fixedSeedPseudoRandom));
      }
      CompletableFuture<Void> voidCompletableFuture = remoteCache.putAllAsync(bulkPut);
      await( voidCompletableFuture, SIZE * 2 );
   }
}
