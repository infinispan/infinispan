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
