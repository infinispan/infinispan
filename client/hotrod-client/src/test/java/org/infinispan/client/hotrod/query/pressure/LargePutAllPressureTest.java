package org.infinispan.client.hotrod.query.pressure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.CHUNK_SIZE;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.chunk;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Sale;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.pressure.LargePutAllPressureTest")
public class LargePutAllPressureTest extends SingleHotRodServerTest {

   private static final Log log = LogFactory.getLog(LargePutAllPressureTest.class);

   private static final int SIZE = 500;
   private static final int TIMEOUT = (int) TimeUnit.MINUTES.toMillis(1) ;

   private final Random fixedSeedPseudoRandom = new Random(739);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.LOCAL, useTransactions()));
      config.indexing().enable()
            .storage(LOCAL_HEAP)
               .addIndexedEntity("Sale")
            .writer()
               .queueCount(10)
               .queueSize(100);

      return TestCacheManagerFactory.createServerModeCacheManager(contextInitializer(), config);
   }

   protected boolean useTransactions() {
      return false;
   }

   @Override
   protected HotRodServer createHotRodServer() {
      return HotRodTestingUtil.startHotRodServer(cacheManager, "127.0.0.1", ServerTestingUtil.findFreePort(),
            new HotRodServerConfigurationBuilder(), false);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder
      createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            super.createHotRodClientConfigurationBuilder(host, serverPort);

      builder.socketTimeout(TIMEOUT);
      return builder;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Sale.SaleSchema.INSTANCE;
   }

   @Test
   public void test() {
      long start = System.currentTimeMillis();

      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      int days = SIZE / CHUNK_SIZE;
      HashMap<String, Sale> bulkPut = new HashMap<>(SIZE);
      for (int day = 1; day <= days; day++) {
         bulkPut.putAll(chunk(day, fixedSeedPseudoRandom));
      }
      CompletableFuture<Void> voidCompletableFuture = remoteCache.putAllAsync(bulkPut);
      await( voidCompletableFuture, TIMEOUT );

      long end = System.currentTimeMillis();
      log.info("massive put all executed in " + (end - start) + "ms");

      Query<Object[]> query = remoteCache.query("select count(s) from Sale s");
      List<Object[]> list = query.list();
      assertThat(list).extracting(array -> array[0]).containsExactly((long) SIZE);
   }
}
