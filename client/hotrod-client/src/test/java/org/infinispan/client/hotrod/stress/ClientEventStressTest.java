package org.infinispan.client.hotrod.stress;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "client.hotrod.event.ClientEventStressTest", timeOut = 15*60*1000)
public class ClientEventStressTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(ClientEventStressTest.class);

   static int NUM_CLIENTS = 3;
   static int NUM_THREADS_PER_CLIENT = 10;
   static final int NUM_OPERATIONS = 10_000;
   static final int NUM_EVENTS = NUM_OPERATIONS * NUM_THREADS_PER_CLIENT * NUM_CLIENTS * NUM_CLIENTS;

   static ExecutorService EXEC = Executors.newCachedThreadPool();

   HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
   }

   RemoteCacheManager getRemoteCacheManager(int port) {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(port);
      return new InternalRemoteCacheManager(builder.build());
   }

   public void testStressEvents() {
      CyclicBarrier barrier = new CyclicBarrier((NUM_CLIENTS * NUM_THREADS_PER_CLIENT) + 1);
      List<Future<Void>> futures = new ArrayList<>(NUM_CLIENTS * NUM_THREADS_PER_CLIENT);
      List<ClientEntryListener> listeners = new ArrayList<>(NUM_CLIENTS);

      RemoteCacheManager[] remotecms = new RemoteCacheManager[NUM_CLIENTS];
      for (int i = 0; i < NUM_CLIENTS; i++)
         remotecms[i] = getRemoteCacheManager(hotrodServer.getPort());

      for (RemoteCacheManager remotecm : remotecms) {
         RemoteCache<Integer, Integer> remote = remotecm.getCache();
         ClientEntryListener listener = new ClientEntryListener();
         listeners.add(listener);
         remote.addClientListener(listener);
         for (int i = 0; i < NUM_THREADS_PER_CLIENT; i++) {
            Callable<Void> call = new Put(barrier, remote);
            futures.add(EXEC.submit(call));
         }
      }
      barrierAwait(barrier); // wait for all threads to be ready
      barrierAwait(barrier); // wait for all threads to finish

      for (Future<Void> f : futures)
         futureGet(f);

      log.debugf("Put operations completed, wait for events...");

      eventuallyEquals(NUM_EVENTS, () -> countEvents(listeners));
   }

   int countEvents(List<ClientEntryListener> listeners) {
      Integer count = listeners.stream().reduce(0, (acc, l) -> acc + l.count.get(), (x, y) -> x + y);
      log.debugf("Event count is %d, target %d%n", (int) count, NUM_EVENTS);
      return count;
   }

   static class Put implements Callable<Void> {
      static final ThreadLocalRandom R = ThreadLocalRandom.current();

      final CyclicBarrier barrier;
      final RemoteCache<Integer, Integer> remote;

      public Put(CyclicBarrier barrier, RemoteCache<Integer, Integer> remote) {
         this.barrier = barrier;
         this.remote = remote;
      }

      @Override
      public Void call() throws Exception {
         barrierAwait(barrier);
         try {
            for (int i = 0; i < NUM_OPERATIONS; i++) {
               int value = R.nextInt(Integer.MAX_VALUE);
               remote.put(value, value); // TODO: Consider using async puts
            }
            return null;
         } finally {
            barrierAwait(barrier);
         }
      }
   }

   @ClientListener
   static class ClientEntryListener {
      final AtomicInteger count = new AtomicInteger();

      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      @SuppressWarnings("unused")
      public void handleClientEvent(ClientEvent event) {
         int countSoFar;
         if ((countSoFar = count.incrementAndGet()) % 100 == 0) {
            log.debugf("Reached %s", countSoFar);
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
