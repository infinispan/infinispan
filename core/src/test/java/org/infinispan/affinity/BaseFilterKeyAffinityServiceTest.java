package org.infinispan.affinity;

import junit.framework.Assert;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class BaseFilterKeyAffinityServiceTest extends BaseKeyAffinityServiceTest {

   private static Log log = LogFactory.getLog(BaseFilterKeyAffinityServiceTest.class);

   protected EmbeddedCacheManager cacheManager;

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      createService();
   }

   protected abstract void createService();

   protected abstract List<Address> getAddresses() ;


   protected void testSingleKey() throws InterruptedException {
      Map<Address, BlockingQueue> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      assertEquals(getAddresses().size(), blockingQueueMap.keySet().size());
      assertEventualFullCapacity(getAddresses());
   }

   protected void testAddNewServer() throws Exception {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, configuration);
      Cache cache = cm.getCache(cacheName);
      caches.add(cache);
      waitForClusterToResize();
      assertUnaffected();
   }

   protected void testRemoveServers() throws InterruptedException {
      caches.get(4).getCacheManager().stop();
      caches.get(3).getCacheManager().stop();
      caches.remove(4);
      caches.remove(3);
      Assert.assertEquals(3, caches.size());
      waitForClusterToResize();
      assertUnaffected();
   }

   protected void testShutdownOwnManager() throws InterruptedException {
      log.info("**** here it starts");
      caches.get(0).getCacheManager().stop();
      caches.remove(0);
      Assert.assertEquals(2, caches.size());
      TestingUtil.blockUntilViewsReceived(10000, caches);
      Assert.assertEquals(2, topology().size());

      for (int i = 0; i < 10; i++) {
         if (!keyAffinityService.isStarted()) break;
         Thread.sleep(1000);
      }
      assert !keyAffinityService.isStarted();
   }

   private void assertUnaffected() throws InterruptedException {
      for (int i = 0; i < 10; i++) {
         assert keyAffinityService.getAddress2KeysMapping().keySet().size() == getAddresses().size();
         Thread.sleep(200);
      }
      assertEventualFullCapacity(getAddresses());
      assertKeyAffinityCorrectness(getAddresses());
   }
}
