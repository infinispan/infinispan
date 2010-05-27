package org.infinispan.affinity;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.ConsistentHash;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "affinity.KeyAffinityServiceTest")
public class KeyAffinityServiceTest extends BaseKeyAffinityServiceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      assertEquals(4, topology(caches.get(0).getCacheManager()).size());
      assertEquals(4, topology(caches.get(1).getCacheManager()).size());
      assertEquals(4, topology(caches.get(2).getCacheManager()).size());
      assertEquals(4, topology(caches.get(3).getCacheManager()).size());

      cache(0, cacheName).put("k", "v");
      assertEquals("v", cache(0, cacheName).get("k"));
      assertEquals("v", cache(1, cacheName).get("k"));
      assertEquals("v", cache(2, cacheName).get("k"));
      assertEquals("v", cache(3, cacheName).get("k"));


      ThreadFactory tf = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "KeyGeneratorThread");
         }
      };
      keyAffinityService = (KeyAffinityServiceImpl) KeyAffinityServiceFactory.newKeyAffinityService(manager(0).getCache(cacheName),
                                                                                                    Executors.newSingleThreadExecutor(tf),
                                                                                                    new RndKeyGenerator(), 100);
   }

   public void testKeysAreCorrectlyCreated() throws Exception {
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test (dependsOnMethods = "testKeysAreCorrectlyCreated")
   public void testConcurrentConsumptionOfKeys() throws InterruptedException {
      List<KeyConsumer> consumers = new ArrayList<KeyConsumer>();
      int keysToConsume = 1000;
      CountDownLatch consumersStart = new CountDownLatch(1);
      for (int i = 0; i < 10; i++) {
         consumers.add(new KeyConsumer(keysToConsume, consumersStart));
      }
      consumersStart.countDown();

      for (KeyConsumer kc : consumers) {
         kc.join();
      }

      for (KeyConsumer kc : consumers) {
         assertEquals(null, kc.exception);
      }

      assertEventualFullCapacity();
   }

   @Test (dependsOnMethods = "testConcurrentConsumptionOfKeys")
   public void testServerAdded() throws InterruptedException {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, configuration);
      Cache cache = cm.getCache(cacheName);
      caches.add(cache);
      waitForClusterToResize();
      for (int i = 0; i < 10; i++) {
         if (keyAffinityService.getAddress2KeysMapping().keySet().size() == 5) {
            break;
         }
         Thread.sleep(500);
      }
      assertEquals(5, keyAffinityService.getAddress2KeysMapping().keySet().size());
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test(dependsOnMethods = "testServerAdded")
   public void testServersDropped() throws InterruptedException {
      log.info("*** Here it is");
      caches.get(4).getCacheManager().stop();
      caches.remove(4);
      waitForClusterToResize();
      for (int i = 0; i < 10; i++) {
         if (keyAffinityService.getAddress2KeysMapping().keySet().size() == 3) {
            break;
         }
         Thread.sleep(500);
      }
      assertEquals(4, keyAffinityService.getAddress2KeysMapping().keySet().size());
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test (dependsOnMethods = "testServersDropped")
   public void testCollocatedKey() {
      ConsistentHash hash = manager(0).getCache(cacheName).getAdvancedCache().getDistributionManager().getConsistentHash();
      for (int i = 0; i < 1000; i++) {
         List<Address> addresses = hash.locate(i, numOwners);
         Object collocatedKey = keyAffinityService.getCollocatedKey(i);
         List<Address> addressList = hash.locate(collocatedKey, numOwners);
         assertEquals(addresses, addressList);
      }
   }

   public class KeyConsumer extends Thread {

      volatile Exception exception;


      private final int keysToConsume;
      private CountDownLatch consumersStart;
      private final List<Address> topology = topology();
      private final Random rnd = new Random();

      public KeyConsumer(int keysToConsume, CountDownLatch consumersStart) {
         super("KeyConsumer");
         this.keysToConsume = keysToConsume;
         this.consumersStart = consumersStart;
         start();
      }

      @Override
      public void run() {
         try {
            consumersStart.await();
         } catch (InterruptedException e) {
            e.printStackTrace();
            return;
         }
         for (int i = 0; i < keysToConsume; i++) {
            Address whichAddr = topology.get(rnd.nextInt(topology.size()));
            try {
               Object keyForAddress = keyAffinityService.getKeyForAddress(whichAddr);
               assertMapsToAddress(keyForAddress, whichAddr);
            } catch (Exception e) {
               this.exception = e;
               break;
            }
         }
      }
   }

}
