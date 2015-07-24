package org.infinispan.affinity.impl;

import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterTest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class BaseKeyAffinityServiceTest extends BaseDistFunctionalTest<Object, String> {

   protected ThreadFactory threadFactory = new ThreadFactory() {
      public Thread newThread(Runnable r) {
         return new Thread(r, "KeyGeneratorThread," + BaseKeyAffinityServiceTest.this.getClass().getSimpleName());
      }
   };
   protected ExecutorService executor  = Executors.newSingleThreadExecutor(threadFactory);
   protected KeyAffinityServiceImpl<Object> keyAffinityService;

   @AfterTest
   public void stopExecutorService() throws InterruptedException {
      if (keyAffinityService != null) keyAffinityService.stop();
      if (executor != null) {
         executor.shutdown();
         boolean terminatedGracefully = executor.awaitTermination(100, TimeUnit.MILLISECONDS);
         executor.shutdownNow();
         if (!terminatedGracefully) {
            fail("KeyGenerator Executor not terminated in expected time");
         }
      }
   }

   protected void assertMapsToAddress(Object o, Address addr) {
      ConsistentHash hash = caches.get(0).getAdvancedCache().getDistributionManager().getConsistentHash();
      List<Address> addresses = hash.locateOwners(o);
      assertEquals("Expected key " + o + " to map to address " + addr + ". List of addresses is" + addresses, true, addresses.contains(addr));
   }

   protected List<Address> topology() {
      return topology(caches.get(0).getCacheManager());
   }

   protected List<Address> topology(CacheContainer cm) {
      return cm.getCache(cacheName).getAdvancedCache().getRpcManager().getTransport().getMembers();
   }

   protected void assertEventualFullCapacity() throws InterruptedException {
      List<Address> addresses = topology();
      assertEventualFullCapacity(addresses);
   }

   protected void assertCorrectCapacity() throws InterruptedException {
      assertCorrectCapacity(topology());
   }

   protected void assertEventualFullCapacity(List<Address> addresses) throws InterruptedException {
      Map<Address, BlockingQueue<Object>> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      for (Address addr : addresses) {
         final BlockingQueue<Object> queue = blockingQueueMap.get(addr);
         //the queue will eventually get filled
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() {
               return queue.size() == 100;
            }
         }, 60 * 1000);  // No more than 1 minute per address since any more is ridiculous!
      }

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() {
            return keyAffinityService.getMaxNumberOfKeys() == keyAffinityService.existingKeyCount.get();
         }
      });

      assertEquals(addresses.size() * 100, keyAffinityService.existingKeyCount.get());

      // give the worker thread some time to shut down
      Thread.sleep(200);
      assertEquals(false, keyAffinityService.isKeyGeneratorThreadActive());
   }

   protected void assertCorrectCapacity(List<Address> addresses) throws InterruptedException {
      Map<Address, BlockingQueue<Object>> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      long maxWaitTime = 5 * 60 * 1000;
      for (Address addr : addresses) {
         BlockingQueue<Object> queue = blockingQueueMap.get(addr);
         long giveupTime = System.currentTimeMillis() + maxWaitTime;
         while (queue.size() < KeyAffinityServiceImpl.THRESHOLD * 100 && System.currentTimeMillis() < giveupTime) Thread.sleep(100);
         assert queue.size() >= KeyAffinityServiceImpl.THRESHOLD * 100 : "Obtained " + queue.size();
      }
   }

   protected void assertKeyAffinityCorrectness() {
      List<Address> addressList = topology();
      assertKeyAffinityCorrectness(addressList);
   }

   protected void assertKeyAffinityCorrectness(Collection<Address> addressList) {
      Map<Address, BlockingQueue<Object>> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      for (Address addr : addressList) {
         BlockingQueue<Object> queue = blockingQueueMap.get(addr);
         assertEquals(100, queue.size());
         for (Object o : queue) {
            assertMapsToAddress(o, addr);
         }
      }
   }

   protected void waitForClusterToResize() {
      TestingUtil.blockUntilViewsReceived(10000, false, caches);
      TestingUtil.waitForRehashToComplete(caches);
      assertEquals(caches.size(), topology().size());
   }
}
