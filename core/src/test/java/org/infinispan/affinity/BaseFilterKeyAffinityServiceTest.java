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

import junit.framework.Assert;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class BaseFilterKeyAffinityServiceTest extends BaseKeyAffinityServiceTest {

   private static final Log log = LogFactory.getLog(BaseFilterKeyAffinityServiceTest.class);

   protected EmbeddedCacheManager cacheManager;


   @Override
   protected void createCacheManagers() throws Throwable {
      INIT_CLUSTER_SIZE = 2;
      super.createCacheManagers();
      createService();
   }

   protected abstract void createService();

   protected abstract List<Address> getAddresses() ;


   protected void testSingleKey() throws InterruptedException {
      Map<Address, BlockingQueue<Object>> blockingQueueMap = keyAffinityService.getAddress2KeysMapping();
      assertEquals(getAddresses().size(), blockingQueueMap.keySet().size());
      assertEventualFullCapacity(getAddresses());
   }

   protected void testAddNewServer() throws Exception {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, configuration);
      Cache<Object, String> cache = cm.getCache(cacheName);
      caches.add(cache);
      waitForClusterToResize();
      assertUnaffected();
   }

   protected void testRemoveServers() throws InterruptedException {
      log.info("** before calling stop");
      caches.get(2).getCacheManager().stop();
      caches.remove(2);
      waitForClusterToResize();
      Assert.assertEquals(2, caches.size());
      assertUnaffected();
   }

   protected void testShutdownOwnManager() {
      log.info("**** here it starts");
      caches.get(0).getCacheManager().stop();
      caches.remove(0);
      Assert.assertEquals(1, caches.size());
      TestingUtil.blockUntilViewsReceived(10000, false, caches.toArray(new Cache[0]));
      Assert.assertEquals(1, topology().size());

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !keyAffinityService.isStarted();
         }
      });
   }

   private void assertUnaffected() throws InterruptedException {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return keyAffinityService.getAddress2KeysMapping().keySet().size() == getAddresses().size();
         }
      });
      assertEventualFullCapacity(getAddresses());
      assertKeyAffinityCorrectness(getAddresses());
   }
}
