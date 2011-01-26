package org.infinispan.affinity;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class BaseKeyAffinityServiceTest extends BaseDistFunctionalTest {
   
   protected KeyAffinityServiceImpl keyAffinityService;

   protected void assertMapsToAddress(Object o, Address addr) {
      ConsistentHash hash = caches.get(0).getAdvancedCache().getDistributionManager().getConsistentHash();
      List<Address> addresses = hash.locate(o, numOwners);
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

   protected void assertEventualFullCapacity(List<Address> addresses) throws InterruptedException {
      Map<Address, BlockingQueue> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      for (Address addr : addresses) {
         BlockingQueue queue = blockingQueueMap.get(addr);
         //the queue will eventually get filled
         for (int i = 0; i < 30; i++) {
            if (!(queue.size() == 100)) {
               Thread.sleep(1000);
            } else {
               break;
            }
         }
         assertEquals(100, queue.size());
      }
      assertEquals(keyAffinityService.getMaxNumberOfKeys(), keyAffinityService.getExitingNumberOfKeys());
      assertEquals(addresses.size() * 100, keyAffinityService.getExitingNumberOfKeys());
      assertEquals(false, keyAffinityService.isKeyGeneratorThreadActive());
   }

   protected void assertKeyAffinityCorrectness() {
      List<Address> addressList = topology();
      assertKeyAffinityCorrectness(addressList);
   }

   protected void assertKeyAffinityCorrectness(Collection<Address> addressList) {
      Map<Address, BlockingQueue> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      for (Address addr : addressList) {
         BlockingQueue queue = blockingQueueMap.get(addr);
         assertEquals(100, queue.size());
         for (Object o : queue) {
            assertMapsToAddress(o, addr);
         }
      }
   }

   protected void waitForClusterToResize() {
      TestingUtil.blockUntilViewsReceived(10000, caches);
      RehashWaiter.waitForInitRehashToComplete(new HashSet<Cache>(caches));
      assertEquals(caches.size(), topology().size());
   }
}
