package org.infinispan.affinity.impl;

import org.infinispan.Cache;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "affinity.KeyAffinityServiceTest")
public class KeyAffinityServiceTest extends BaseKeyAffinityServiceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      super.INIT_CLUSTER_SIZE = 2;
      super.createCacheManagers();
      assertEquals(2, topology(caches.get(0).getCacheManager()).size());
      assertEquals(2, topology(caches.get(1).getCacheManager()).size());

      cache(0, cacheName).put("k", "v");
      assertEquals("v", cache(0, cacheName).get("k"));
      assertEquals("v", cache(1, cacheName).get("k"));


      keyAffinityService = (KeyAffinityServiceImpl<Object>) KeyAffinityServiceFactory.newKeyAffinityService(manager(0).getCache(cacheName),
            executor, new RndKeyGenerator(), 100);
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

      assertCorrectCapacity();
   }

   @Test (dependsOnMethods = "testConcurrentConsumptionOfKeys")
   public void testServerAdded() throws InterruptedException {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, configuration.build());
      Cache<Object, String> cache = cm.getCache(cacheName);
      caches.add(cache);
      waitForClusterToResize();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
           return keyAffinityService.getAddress2KeysMapping().keySet().size() == 3;
         }
      });
      assertEquals(3, keyAffinityService.getAddress2KeysMapping().keySet().size());
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test(dependsOnMethods = "testServerAdded")
   public void testServersDropped() throws InterruptedException {
      caches.get(2).getCacheManager().stop();
      caches.remove(2);
      waitForClusterToResize();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return keyAffinityService.getAddress2KeysMapping().keySet().size() == 2;
         }
      });
      assertEquals(2, keyAffinityService.getAddress2KeysMapping().keySet().size());
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test (dependsOnMethods = "testServersDropped")
   public void testCollocatedKey() {
      ConsistentHash hash = manager(0).getCache(cacheName).getAdvancedCache().getDistributionManager().getConsistentHash();
      for (int i = 0; i < 1000; i++) {
         List<Address> addresses = hash.locateOwners(i);
         Object collocatedKey = keyAffinityService.getCollocatedKey(i);
         List<Address> addressList = hash.locateOwners(collocatedKey);
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
            log.debug("KeyConsumer thread interrupted");
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
