package org.infinispan.affinity.impl;

import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.impl.KeyAffinityServiceImpl;
import org.infinispan.affinity.impl.RndKeyGenerator;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *  This class just overrides the methods in the base class as TestNG behaves funny with depending methods and inheritance.
 * 
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "functional", testName = "affinity.FilteredKeyAffinityServiceTest")
public class FilteredKeyAffinityServiceTest extends BaseFilterKeyAffinityServiceTest {
   private List<Address> filter;

   @Override
   protected void createService() {
      filter = new ArrayList<Address>();
      filter.add(caches.get(0).getAdvancedCache().getRpcManager().getTransport().getAddress());
      filter.add(caches.get(1).getAdvancedCache().getRpcManager().getTransport().getAddress());
      cacheManager = caches.get(0).getCacheManager();
      keyAffinityService = (KeyAffinityServiceImpl<Object>) KeyAffinityServiceFactory.
            newKeyAffinityService(cacheManager.getCache(cacheName), filter, new RndKeyGenerator(),
                  executor, 100);
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
   public void testShutdownOwnManager() {
      super.testShutdownOwnManager();
   }   
}
