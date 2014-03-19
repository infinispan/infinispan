package org.infinispan.affinity.impl;

import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.impl.KeyAffinityServiceImpl;
import org.infinispan.affinity.impl.RndKeyGenerator;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

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
         cacheManager = caches.get(0).getCacheManager();
         keyAffinityService = (KeyAffinityServiceImpl<Object>) KeyAffinityServiceFactory.
               newLocalKeyAffinityService(cacheManager.getCache(cacheName), new RndKeyGenerator(),
                     executor, 100);
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
   public void testShutdownOwnManager() {
      super.testShutdownOwnManager();
   }
}
