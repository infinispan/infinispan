package org.infinispan.distribution.rehash;

import static java.lang.String.format;
import static org.infinispan.test.TestingUtil.sleepRandom;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

// As this is a SLOW stress test, leave it disabled by default.  Only run it manually.
@Test(groups = "stress", testName = "distribution.rehash.ConsistencyStressTest", timeOut = 15*60*1000)
public class ConsistencyStressTest extends MultipleCacheManagersTest {
   private static final int NUM_NODES = 10;
   private static final int WORKERS_PER_NODE = 2;
   private static final int NUM_ITERATIONS = 5000;
   private static final boolean IGNORE_TX_FAILURES = true;
   private static final Log log = LogFactory.getLog(ConsistencyStressTest.class);


   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c
         .locking()
            .isolationLevel(IsolationLevel.READ_COMMITTED)
            .lockAcquisitionTimeout(60000)
            .useLockStriping(false)
         .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .remoteTimeout(30000)
            .l1().disable()
         .transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gc.transport().distributedSyncTimeout(60000);

      List<EmbeddedCacheManager> cacheManagers = new LinkedList<>();

      for (int i = 0; i < NUM_NODES; i++)
         cacheManagers.add(createClusteredCacheManager(gc, c));

      registerCacheManager(cacheManagers.toArray(new EmbeddedCacheManager[NUM_NODES]));
   }

   public void testConsistency() throws Throwable {
      Set<Future<Void>> futures = new HashSet<>(NUM_NODES * WORKERS_PER_NODE);
      Set<String> keysToIgnore = new HashSet<>();

      for (int i = 0; i < NUM_NODES; i++) {
         Cache<String, String> c = cache(i);
         for (int j = 0; j < WORKERS_PER_NODE; j++) {
            Future<Void> f = fork(new Stressor(c, i, j, keysToIgnore));
            futures.add(f);
            sleepRandom(500);
         }
      }

      // stressors are now running, generating a lot of data.
      // wait for all stressors to finish.
      log.info("Waiting for stressors to finish");
      for (Future<Void> f : futures) f.get();

      // Now shut down a node:
      TestingUtil.killCacheManagers(cacheManagers.get(0));

      // ... and ensure no data is lost.
      // Stressors encode data in the format nodeNumber|workerNumber|iterationNumber, and all have the value "value".

      Map<Address, Cache<Object, Object>> cacheMap = new HashMap<>();
      for (int i = 1; i < NUM_NODES; i++) {
         Cache<Object, Object> c = cache(i);
         cacheMap.put(address(c), c);
      }

      // Let's enforce a quiet period to allow queued up transactions to complete.
      Thread.sleep(25000);

      // lets make sure any rehashing work has completed
      TestingUtil.blockUntilViewsReceived(60000, false, cacheMap.values());
      TestingUtil.waitForNoRebalance(cacheMap.values());
      LocalizedCacheTopology cacheTopology = cache(1).getAdvancedCache().getDistributionManager().getCacheTopology();

      for (int i = 0; i < NUM_NODES; i++) {
         for (int j = 0; j < WORKERS_PER_NODE; j++) {
            for (int k = 0; k < NUM_ITERATIONS; k++) {
               String key = keyFor(i, j, k);
               if (keysToIgnore.contains(key)) {
                  log.infof("Skipping test on failing key %s", key);
               } else {
                  Collection<Address> owners = cacheTopology.getWriteOwners(key);
                  for (Map.Entry<Address, Cache<Object, Object>> e : cacheMap.entrySet()) {
                     try {
                        if (owners.contains(e.getKey())) {
                           DistributionTestHelper.assertIsInContainerImmortal(e.getValue(), key);
                        }
                        // Don't bother testing non-owners since invalidations caused by rehashing are async!
                     } catch (Throwable th) {
                        log.fatalf("Key %s (segment %s) should be on owners %s according to %s",
                                   key, cacheTopology.getSegment(key), owners, cacheTopology);
                        throw th;
                     }
                  }
               }
            }
         }
      }
   }

   private static String keyFor(int nodeId, int workerId, int iterationId) {
      return format("__%s_%s_%s__", nodeId, workerId, iterationId);
   }

   private static class Stressor implements Callable<Void> {
      private final Cache<String, String> cache;
      private final TransactionManager tm;
      private final int cacheId, workerId;
      private final Set<String> keysToIgnore;

      private Stressor(Cache<String, String> cache, int cacheId, int workerId, Set<String> keysToIgnore) {
         this.cache = cache;
         tm = TestingUtil.getTransactionManager(cache);
         this.cacheId = cacheId;
         this.workerId = workerId;
         this.keysToIgnore = keysToIgnore;
      }

      @Override
      public Void call() throws TimeoutException {
         for (int iterationId = 0; iterationId < NUM_ITERATIONS; iterationId++) {
            if (iterationId % 500 == 0)
               log.infof("  >> Stressor %s Worker %s Iteration %s", cacheId, workerId, iterationId);
            boolean txError = false;
            Exception exception = null;
            String key = keyFor(cacheId, workerId, iterationId);

            try {
               tm.begin();
               cache.getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).put(key, "value");
               tm.commit();
            } catch (HeuristicRollbackException | RollbackException | SystemException | HeuristicMixedException | NotSupportedException | TimeoutException e) {
               txError = true;
               exception = e;
            }

            if (txError) {
               //first try and roll back the tx
               try {
                  tm.rollback();
               } catch (Exception exc) {
                  // rollback failed?
                  log.error("  >> Rollback failed");
               }

               if (IGNORE_TX_FAILURES) {
                  keysToIgnore.add(key);
                  log.errorf("  >> Saw a %s when trying to process key %s", exception.getClass().getSimpleName(), key);
               } else {
                  throw new RuntimeException(exception);
               }
            }
         }
         return null;
      }
   }
}
