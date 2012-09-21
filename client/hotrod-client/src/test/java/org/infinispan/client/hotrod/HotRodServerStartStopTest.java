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

import org.infinispan.config.Configuration;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.HotRodServerStartStopTest", groups = "functional")
public class HotRodServerStartStopTest extends MultipleCacheManagersTest {
   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      addClusterEnabledCacheManager(config);
      addClusterEnabledCacheManager(config);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));

      assert manager(0).getCache() != null;
      assert manager(1).getCache() != null;

      waitForClusterToForm();

      cache(0).put("k","v");
      assertEquals("v", cache(1).get("k"));
   }

   public void testTouchServer() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager("localhost", hotRodServer1.getPort(), true);
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("k", "v");
      assertEquals("v", remoteCache.get("k"));
   }

   @Test (dependsOnMethods = "testTouchServer")
   public void testHrServerStop() {
      hotRodServer1.stop();
      hotRodServer2.stop();
   }
}
