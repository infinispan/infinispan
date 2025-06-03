package org.infinispan.client.hotrod.stress.near;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManagers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Manual test, requires external Hot Rod server.
 */
@Test(groups = "manual", testName = "client.hotrod.stress.near.EagerNearCacheStressTest")
public class EagerNearCacheStressTest {

   static int NUM_CLIENTS = 3;
   static int NUM_THREADS_PER_CLIENT = 10;
   static ExecutorService EXEC = Executors.newCachedThreadPool();

   static final int NUM_OPERATIONS = 10_000_000;
   static final int NUM_KEYS_PRELOAD = 1_000;
   static final int KEY_RANGE = 1_000;

   @AfterClass
   public static void shutdownExecutor() {
      EXEC.shutdown();
   }

   EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
   }

   RemoteCacheManager getRemoteCacheManager(int port) {
      return getRemoteCacheManager(port, NearCacheMode.DISABLED, -1);
   }

   RemoteCacheManager getRemoteCacheManager(int port, NearCacheMode nearCacheMode, int maxEntries) {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.remoteCache("").nearCacheMode(nearCacheMode).nearCacheMaxEntries(maxEntries);
      builder.addServer().host("127.0.0.1").port(port);
      return new RemoteCacheManager(builder.build());
   }

   public void testLocalPreloadAndGetPut10to1() {
      runPreloadAndOps(NearCacheMode.INVALIDATED, -1, 0.90);
   }

   void runPreloadAndOps(NearCacheMode nearCacheMode, int maxEntries, double getRatio) {
      EmbeddedCacheManager cm = createCacheManager();
      //HotRodServer server = createHotRodServer(cm);
      int port = 11222;
      preloadData(port);
      RemoteCacheManager[] remotecms = new RemoteCacheManager[NUM_CLIENTS];
      for (int i = 0; i < NUM_CLIENTS; i++)
         remotecms[i] = getRemoteCacheManager(port, nearCacheMode, maxEntries);
      try {
         ops(remotecms, getRatio);
      } finally {
         killRemoteCacheManagers(remotecms);
         //killServers(server);
         TestingUtil.killCacheManagers(cm);
      }
   }

   void ops(RemoteCacheManager[] remotecms, double getRatio) {
      CyclicBarrier barrier = new CyclicBarrier((NUM_CLIENTS * NUM_THREADS_PER_CLIENT) + 1);
      List<Future<Void>> futures = new ArrayList<>(NUM_CLIENTS * NUM_THREADS_PER_CLIENT);
      for (RemoteCacheManager remotecm : remotecms) {
         RemoteCache<Integer, String> remote = remotecm.getCache();
         for (int i = 0; i < NUM_THREADS_PER_CLIENT; i++) {
            Callable<Void> call = new Main(barrier, remote, getRatio);
            futures.add(EXEC.submit(call));
         }
      }
      barrierAwait(barrier); // wait for all threads to be ready
      barrierAwait(barrier); // wait for all threads to finish

      for (Future<Void> f : futures)
         futureGet(f);
   }

   void preloadData(int port) {
      // Preload data
      withRemoteCacheManager(new RemoteCacheManagerCallable(getRemoteCacheManager(port)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> remote = rcm.getCache();
            Map<Integer, String> map = new HashMap<>();
            for (int i = 0; i < NUM_KEYS_PRELOAD; ++i)
               map.put(i, TestingUtil.generateRandomString(512));
            remote.putAll(map);
         }
      });
   }

   abstract static class Runner implements Callable<Void> {

      final CyclicBarrier barrier;
      final RemoteCache<Integer, String> remote;
      final double getRatio;

      Runner(CyclicBarrier barrier, RemoteCache<Integer, String> remote, double getRatio) {
         this.barrier = barrier;
         this.remote = remote;
         this.getRatio = getRatio;
      }

      @Override
      public Void call() throws Exception {
         barrierAwait(barrier);
         try {
            run();
            return null;
         } finally {
            barrierAwait(barrier);
         }
      }

      abstract void run();
   }

   static final class Main extends Runner {
      static final ThreadLocalRandom R = ThreadLocalRandom.current();

      Main(CyclicBarrier barrier, RemoteCache<Integer, String> remote, double getRatio) {
         super(barrier, remote, getRatio);
      }

      @Override
      void run() {
         double maxGetKey = KEY_RANGE * getRatio;
         for (int i = 0; i < NUM_OPERATIONS; i++) {
            int key = R.nextInt(KEY_RANGE);
            if (key < maxGetKey) {
               String value = remote.get(key);
               assertNotNull(value);
            } else {
               String prev = remote.put(key, TestingUtil.generateRandomString(512));
               assertNull(prev);
            }
         }
      }
   }

   static int barrierAwait(CyclicBarrier barrier) {
      try {
         return barrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
         throw new AssertionError(e);
      }
   }

   <T> T futureGet(Future<T> future) {
      try {
         return future.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new AssertionError(e);
      }
   }

}
