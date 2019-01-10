package org.infinispan.client.hotrod.stress;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.toBytes;
import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;
import static org.infinispan.distribution.DistributionTestHelper.isOwner;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "client.hotrod.event.ClusterClientEventStressTest", timeOut = 15*60*1000)
public class ClusterClientEventStressTest extends MultiHotRodServersTest {

   private static final Log log = LogFactory.getLog(ClusterClientEventStressTest.class);

   static final int NUM_SERVERS = 3;
   static final int NUM_OWNERS = 2;

   static final int NUM_CLIENTS = 1;
   static final int NUM_THREADS_PER_CLIENT = 6;
//   static final int NUM_THREADS_PER_CLIENT = 36;

   static final int NUM_OPERATIONS = 1_000; // per thread, per client
//   static final int NUM_OPERATIONS = 300; // per thread, per client
//   static final int NUM_OPERATIONS = 600; // per thread, per client

   static final int NUM_EVENTS = NUM_OPERATIONS * NUM_THREADS_PER_CLIENT * NUM_CLIENTS * NUM_CLIENTS;

//   public static final int NUM_STORES = NUM_OPERATIONS * NUM_CLIENTS * NUM_THREADS_PER_CLIENT;
//   public static final int NUM_STORES_PER_SERVER = NUM_STORES / NUM_SERVERS;
//   public static final int NUM_ENTRIES_PER_SERVER = (NUM_STORES * NUM_OWNERS) / NUM_SERVERS;

   static Set<String> ALL_KEYS = new ConcurrentHashSet<>();

   static ExecutorService EXEC = Executors.newCachedThreadPool();

   static ClientEntryListener listener;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(NUM_OWNERS)
//            .expiration().lifespan(1000).maxIdle(1000).wakeUpInterval(5000)
            .expiration().maxIdle(1000).wakeUpInterval(5000)
            .jmxStatistics().enable();
      return hotRodCacheConfiguration(builder);
   }

   RemoteCacheManager getRemoteCacheManager(int port) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(port);
      RemoteCacheManager rcm = new InternalRemoteCacheManager(builder.build());
      rcm.getCache();
      return rcm;
   }

   Map<String, RemoteCacheManager> createClients() {
      Map<String, RemoteCacheManager> remotecms = new HashMap<>(NUM_CLIENTS);
      for (int i = 0; i < NUM_CLIENTS; i++)
         remotecms.put("c" + i, getRemoteCacheManager(server(0).getPort()));

      return remotecms;
   }

   public void testAddClientListenerDuringOperations() {
      CyclicBarrier barrier = new CyclicBarrier((NUM_CLIENTS * NUM_THREADS_PER_CLIENT) + 1);
      List<Future<Void>> futures = new ArrayList<>(NUM_CLIENTS * NUM_THREADS_PER_CLIENT);
      List<ClientEntryListener> listeners = new ArrayList<>(NUM_CLIENTS);
      Map<String, RemoteCacheManager> remotecms = createClients();

//      assertStatsBefore(remotecms);

      for (Entry<String, RemoteCacheManager> e : remotecms.entrySet()) {
         RemoteCache<String, String> remote = e.getValue().getCache();

//         ClientEntryListener listener = new ClientEntryListener();
//         listeners.add(listener);
//         remote.addClientListener(listener);

         for (int i = 0; i < NUM_THREADS_PER_CLIENT; i++) {
            String prefix = String.format("%s-t%d-", e.getKey(), i);
            Callable<Void> call = new Put(prefix, barrier, remote, servers);
            futures.add(EXEC.submit(call));
         }
      }

      barrierAwait(barrier); // wait for all threads to be ready
      barrierAwait(barrier); // wait for all threads to finish

      for (Future<Void> f : futures)
         futureGet(f);

//      log.debugf("Put operations completed, assert statistics");
//      assertStatsAfter(remotecms);

//      log.debugf("Stats asserted, wait for events...");
//      eventuallyEquals(NUM_EVENTS, () -> countEvents(listeners));
   }

   int countEvents(List<ClientEntryListener> listeners) {
      Integer count = listeners.stream().reduce(0, (acc, l) -> acc + l.count.get(), (x, y) -> x + y);
      log.infof("Event count is %d, target %d%n", (int) count, NUM_EVENTS);
      return count;
   }

//   void assertStatsBefore(Map<String, RemoteCacheManager> remotecms) {
//      RemoteCacheManager client = remotecms.values().iterator().next();
//      IntStream.range(0, NUM_SERVERS)
//            .forEach(x -> {
//               ServerStatistics stat = client.getCache().stats();
//               assertEquals(0, stat.getIntStatistic(STORES).intValue());
//               assertEquals(0, stat.getIntStatistic(CURRENT_NR_OF_ENTRIES).intValue());
//            });
//   }
//
//   void assertStatsAfter(Map<String, RemoteCacheManager> remotecms) {
//      RemoteCacheManager client = remotecms.values().iterator().next();
//      IntStream.range(0, NUM_SERVERS)
//            .forEach(x -> {
//               ServerStatistics stat = client.getCache().stats();
//               int stores = stat.getIntStatistic(STORES);
//               System.out.println("Number of stores: " + stores);
//               assertEquals(NUM_STORES_PER_SERVER, stores);
//               int entries = stat.getIntStatistic(CURRENT_NR_OF_ENTRIES);
//               assertEquals(NUM_ENTRIES_PER_SERVER, entries);
//            });
//   }

   static class Put implements Callable<Void> {
      static final ThreadLocalRandom R = ThreadLocalRandom.current();

      final CyclicBarrier barrier;
      final RemoteCache<String, String> remote;
      final List<HotRodServer> servers;
      final List<String> keys;

      public Put(String prefix, CyclicBarrier barrier,
            RemoteCache<String, String> remote, List<HotRodServer> servers) {
         this.barrier = barrier;
         this.remote = remote;
         this.servers = servers;
         this.keys = generateKeys(prefix, servers, R);
         Collections.shuffle(this.keys);
      }

      @Override
      public Void call() throws Exception {
         barrierAwait(barrier);
         try {
            for (int i = 0; i < NUM_OPERATIONS; i++) {
               String value = keys.get(i);
               remote.put(value, value);
               if (value.startsWith("c0-t0") && i == (NUM_OPERATIONS / 2)) {
                  listener = new ClientEntryListener();
                  remote.addClientListener(listener);
               }
            }
            return null;
         } finally {
            barrierAwait(barrier);
         }
      }
   }

   static List<String> generateKeys(String prefix, List<HotRodServer> servers, ThreadLocalRandom r) {
      List<String> keys = new ArrayList<>();
      List<HotRodServer[]> combos = Arrays.asList(
            new HotRodServer[]{servers.get(0), servers.get(1)},
            new HotRodServer[]{servers.get(1), servers.get(0)},
            new HotRodServer[]{servers.get(1), servers.get(2)},
            new HotRodServer[]{servers.get(2), servers.get(1)},
            new HotRodServer[]{servers.get(2), servers.get(0)},
            new HotRodServer[]{servers.get(0), servers.get(2)}
      );

      for (int i = 0; i < NUM_OPERATIONS; i++) {
         HotRodServer[] owners = combos.get(i % combos.size());
         String key = getStringKey(prefix, owners, r);
         if (ALL_KEYS.contains(key))
            throw new AssertionError("Key already in use: " + key);

         keys.add(key);
         ALL_KEYS.add(key);
      }
      return keys;
   }

   static String getStringKey(String prefix, HotRodServer[] owners, ThreadLocalRandom r) {
      Cache<?, ?> firstOwnerCache = owners[0].getCacheManager().getCache();
      Cache<?, ?> otherOwnerCache = owners[1].getCacheManager().getCache();

//      Random r = new Random();
      byte[] dummy;
      String dummyKey;
      int attemptsLeft = 1000;
      do {
         Integer dummyInt = r.nextInt();
         dummyKey = prefix + dummyInt;
         dummy = toBytes(dummyKey);
         attemptsLeft--;
      } while (!(isFirstOwner(firstOwnerCache, dummy) && isOwner(otherOwnerCache, dummy))
            && attemptsLeft >= 0);

      if (attemptsLeft < 0)
         throw new IllegalStateException("Could not find any key owned by "
               + firstOwnerCache + " as primary owner and "
               + otherOwnerCache + " as secondary owner");

      log.infof("Integer key %s hashes to primary [cluster=%s,hotrod=%s] and secondary [cluster=%s,hotrod=%s]",
            dummyKey,
            firstOwnerCache.getCacheManager().getAddress(), owners[0].getAddress(),
            otherOwnerCache.getCacheManager().getAddress(), owners[1].getAddress());

      return dummyKey;
   }

   static int barrierAwait(CyclicBarrier barrier) {
      try {
         return barrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
         throw new AssertionError(e);
      }
   }

   static <T> T futureGet(Future<T> future) {
      try {
         return future.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new AssertionError(e);
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

}
