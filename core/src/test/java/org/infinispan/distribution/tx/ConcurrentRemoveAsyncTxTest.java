package org.infinispan.distribution.tx;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "distribution.tx.ConcurrentRemoveAsyncTxTest")
public class ConcurrentRemoveAsyncTxTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cb.clustering().hash().numOwners(2);
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createClusteredCaches(2, cb);
   }

   public void testConcurrentRemoveAsyncWithinTx() throws Exception {
      int numClients = 10;
      int numKeys = 30;
      int iterations = 20;
      Cache<String, String> cache = cache(0);

      CyclicBarrier barrier = new CyclicBarrier(numClients);
      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();

      for (int c = 0; c < numClients; c++) {
         int clientId = c;
         CompletableFuture<Void> cs = CompletableFuture.runAsync(() -> {
            try {
               barrier.await(30, TimeUnit.SECONDS);

               for (int iter = 0; iter < iterations; iter++) {
                  String[] keys = new String[numKeys];
                  for (int i = 0; i < numKeys; i++) {
                     String prefix = "c" + clientId + "-i" + iter + "-k" + i;
                     Cache<?, ?> target = (i & 0b1) == 0 ? cache(1) : cache(0);
                     String key = getStringKeyForCache(prefix, target);
                     keys[i] = key;
                  }

                  for (String key : keys) {
                     cache.put(key, "v");
                  }

                  execute(cache, keys);
               }
            } catch (Throwable t) {
               throw CompletableFutures.asCompletionException(t);
            }
         }, testExecutor());
         acs.dependsOn(cs);
      }

      assertThat(acs.freeze().toCompletableFuture().get(30, TimeUnit.SECONDS)).isNull();
   }

   private void execute(Cache<String, String> cache, String[] keys) throws Throwable {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();

      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
      for (String key : keys) {
         CompletionStage<Void> cs = cache.removeAsync(key)
               .thenApply(CompletableFutures.toNullFunction());
         acs.dependsOn(cs);
      }

      acs.freeze().toCompletableFuture().get(30, TimeUnit.SECONDS);
      tm.commit();
   }
}
