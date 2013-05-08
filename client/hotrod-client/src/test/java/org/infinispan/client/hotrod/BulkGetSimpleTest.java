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
package org.infinispan.client.hotrod;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.BulkGetSimpleTest", groups = "functional")
public class BulkGetSimpleTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   private void populateCacheManager() {
      for (int i = 0; i < 100; i++) {
         remoteCache.put(i, i);
      }
   }

   public void testBulkGet() {
      populateCacheManager();
      Map<Object,Object> map = remoteCache.getBulk();
      assert map.size() == 100;
      for (int i = 0; i < 100; i++) {
         assert map.get(i).equals(i);
      }
   }

   public void testBulkGetWithSize() {
      populateCacheManager();
      Map<Object,Object> map = remoteCache.getBulk(50);
      assertEquals(50, map.size());
      for (int i = 0; i < 100; i++) {
         if (map.containsKey(i)) {
            Integer value = (Integer) map.get(i);
            assertEquals((Integer)i, value);
         }
      }
   }

   public void testBulkGetAfterLifespanExpire() throws InterruptedException {
      Map dataIn = new HashMap();
      dataIn.put("aKey", "aValue");
      dataIn.put("bKey", "bValue");
      final long startTime = System.currentTimeMillis();
      final long lifespan = 10000;
      remoteCache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      Map dataOut = new HashMap();
      while (true) {
         dataOut = remoteCache.getBulk();
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assertEquals(dataIn, dataOut);
         Thread.sleep(100);
      }

      // Make sure that in the next 30 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 30000) {
         dataOut = remoteCache.getBulk();
         if (dataOut.size() == 0) return;
      }

      assert dataOut.size() == 0 :
         String.format("Data not empty, it contains: %s elements", dataOut.size());
   }

}
