/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.affinity;

import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.ArrayList;
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
@Test (groups = "functional", testName = "affinity.FilteredKeyAffinityServiceTest")
public class FilteredKeyAffinityServiceTest extends BaseFilterKeyAffinityServiceTest {
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
      cacheManager = caches.get(0).getCacheManager();
      keyAffinityService = (KeyAffinityServiceImpl<Object>) KeyAffinityServiceFactory.
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
   public void testShutdownOwnManager() {
      super.testShutdownOwnManager();
   }   
}
