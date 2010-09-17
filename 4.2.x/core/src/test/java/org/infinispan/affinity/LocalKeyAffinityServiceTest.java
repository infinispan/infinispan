package org.infinispan.affinity;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * This class just overrides the methods in the base class as TestNG behaves funny with depending methods and inheritance.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "affinity.LocalKeyAffinityServiceTest")
public class LocalKeyAffinityServiceTest extends BaseFilterKeyAffinityServiceTest {

   @Override
   protected void createService() {
      {
         ThreadFactory tf = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
               return new Thread(r, "KeyGeneratorThread");
            }
         };
         cacheManager = (EmbeddedCacheManager) caches.get(0).getCacheManager();
         keyAffinityService = (KeyAffinityServiceImpl) KeyAffinityServiceFactory.
               newLocalKeyAffinityService(cacheManager.getCache(cacheName), new RndKeyGenerator(),
                                          Executors.newSingleThreadExecutor(tf), 100);
      }
   }

   @Override
   protected List<Address> getAddresses() {
      return Collections.singletonList(cacheManager.getAddress());
   }

   public void testFilteredSingleKey() throws InterruptedException {
      super.testSingleKey();  
   }

   @Test(dependsOnMethods = "testFilteredSingleKey")
   public void testFilteredAddNewServer() throws Exception {
      super.testAddNewServer();
   }

   @Test(dependsOnMethods = "testFilteredAddNewServer")
   public void testFilteredRemoveServers() throws InterruptedException {
      super.testRemoveServers();
   }

   @Test (dependsOnMethods = "testFilteredRemoveServers")
   public void testShutdownOwnManager() throws InterruptedException {
      super.testShutdownOwnManager();
   }
}
