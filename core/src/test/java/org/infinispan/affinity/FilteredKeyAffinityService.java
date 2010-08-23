package org.infinispan.affinity;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 *
 *  This class just overrides the methods in the base class as TestNG behaves funny with depending methods and inheritance.
 * 
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "functional", testName = "affinity.FilteredKeyAffinityService")
public class FilteredKeyAffinityService extends BaseFilterKeyAffinityServiceTest {
   private List<Address> filter;

   @Override
   protected void createService() {
      ThreadFactory tf = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "KeyGeneratorThread");
         }
      };
      filter = new ArrayList<Address>();
      filter.add(caches.get(0).getAdvancedCache().getRpcManager().getTransport().getAddress());
      filter.add(caches.get(1).getAdvancedCache().getRpcManager().getTransport().getAddress());
      cacheManager = (EmbeddedCacheManager) caches.get(0).getCacheManager();
      keyAffinityService = (KeyAffinityServiceImpl) KeyAffinityServiceFactory.
            newKeyAffinityService(cacheManager.getCache(cacheName), filter, new RndKeyGenerator(),
                                       Executors.newSingleThreadExecutor(tf), 100);
   }

   @Override
   protected List<Address> getAddresses() {
      return filter;
   }

   @Override
   public void testSingleKey() throws InterruptedException {
      super.testSingleKey();  
   }

   @Test(dependsOnMethods = "testSingleKey")
   public void testAddNewServer() throws Exception {
      super.testAddNewServer();
   }

   @Test(dependsOnMethods = "testAddNewServer")
   public void testRemoveServers() throws InterruptedException {
      super.testRemoveServers();
   }

   @Test (dependsOnMethods = "testRemoveServers")
   public void testShutdownOwnManager() throws InterruptedException {
      super.testShutdownOwnManager();
   }   
}
