package org.infinispan.affinity.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.fail;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class BaseKeyAffinityServiceTest extends BaseDistFunctionalTest<Object, String> {

   protected ThreadFactory threadFactory = getTestThreadFactory("KeyGeneratorThread");
   protected ExecutorService executor  = Executors.newSingleThreadExecutor(threadFactory);
   protected KeyAffinityServiceImpl<Object> keyAffinityService;

   @AfterClass(alwaysRun = true)
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
      LocalizedCacheTopology cacheTopology = caches.get(0).getAdvancedCache().getDistributionManager().getCacheTopology();
      List<Address> addresses = cacheTopology.getDistribution(o).writeOwners();
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
      int capacity = 100;
      eventuallyEquals(capacity * addresses.size(), keyAffinityService::getMaxNumberOfKeys);

      Map<Address, BlockingQueue<Object>> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      for (Address addr : addresses) {
         final BlockingQueue<Object> queue = blockingQueueMap.get(addr);
         //the queue will eventually get filled
         eventuallyEquals(capacity, queue::size);
      }
      eventuallyEquals(capacity * addresses.size(), () -> keyAffinityService.existingKeyCount.get());

      // give the worker thread some time to shut down
      Thread.sleep(200);
      assertFalse(keyAffinityService.isKeyGeneratorThreadActive());
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
      TestingUtil.waitForNoRebalance(caches);
      assertEquals(caches.size(), topology().size());
   }
}
