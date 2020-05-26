package org.infinispan.server.functional;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MaxIdleOperations {

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .runMode(ServerRunMode.FORKED)
               .numServers(3)
               .parallelStartup(false)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final int maxIdle;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      // 70_000 is greater than runTestForMs
      return Arrays.asList(new Object[][]{{100}, {1_000}, {10_000}, {70_000}});
   }

   public MaxIdleOperations(int maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Test
   public void testMaxIdleOffHeapOperations() {

      String cacheName = UUID.randomUUID().toString();
      RemoteCache<String, String> remoteCache = SERVER_TEST.hotrod().create().getRemoteCacheManager()
            .administration().getOrCreateCache(cacheName,
            new ConfigurationBuilder()
                  .clustering()
                     .cacheMode(CacheMode.DIST_ASYNC).hash().numOwners(2).hash().numSegments(30)
                  .locking()
                     .isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false).lockAcquisitionTimeout(30000).concurrencyLevel(1000)
                  .transaction()
                     .transactionMode(TransactionMode.NON_TRANSACTIONAL)
                  .memory()
                     .storage(StorageType.OFF_HEAP).maxSize("400000")
                  .expiration()
                     .maxIdle(this.maxIdle).lifespan(300000)
                  .persistence()
                     .passivation(true).addSingleFileStore().maxEntries(1000000).shared(false).preload(false).fetchPersistentState(true).purgeOnStartup(true)
            .build());

      remoteCache.getRemoteCacheManager().getChannelFactory().getNumActive();

      runTest(remoteCache);
   }

   @Test
   public void testMaxIdleOperations() {
      String cacheName = UUID.randomUUID().toString();
      RemoteCache<String, String> remoteCache = SERVER_TEST.hotrod().create().getRemoteCacheManager()
            .administration().getOrCreateCache(cacheName,
            new ConfigurationBuilder()
                  .clustering()
                     .cacheMode(CacheMode.DIST_ASYNC).hash().numOwners(2).hash().numSegments(30)
                  .locking()
                     .isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false).lockAcquisitionTimeout(30000).concurrencyLevel(1000)
                  .transaction()
                     .transactionMode(TransactionMode.NON_TRANSACTIONAL)
                  .expiration()
                     .maxIdle(this.maxIdle).lifespan(300000)
                  .persistence()
                     .passivation(true).addSingleFileStore().maxEntries(1000000).shared(false).preload(false).fetchPersistentState(true).purgeOnStartup(true)
            .build());

      runTest(remoteCache);
   }

   private void runTest(RemoteCache<String, String> remoteCache) {
      int runTestForMs = 30_000;
      int maxKeys = 1000;
      for (int i = 1; i <= maxKeys; i++) {
         remoteCache.put(String.valueOf(i), "Test" + i);
      }
      long begin = System.currentTimeMillis();
      long now = System.currentTimeMillis();
      while (now - begin < runTestForMs) {
         for (int i = 1; i <= maxKeys; i++) {
            String key = String.valueOf(i);
            String result = remoteCache.get(key);
            String message = String.format("Null value for key: %s after %dms", key, now - begin);
            assertNotNull(message, result);
         }
         now = System.currentTimeMillis();
      }
   }
}
