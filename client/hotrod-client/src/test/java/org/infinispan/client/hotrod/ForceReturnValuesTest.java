/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(testName = "client.hotrod.ForceReturnValuesTest", groups = "functional")
@CleanupAfterMethod
public class ForceReturnValuesTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      return cacheManager;
   }

   @AfterMethod(alwaysRun = true)
   void shutdown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotRodServer);
   }

   public void testDontForceReturnValues() {
      RemoteCache<String, String> rc = remoteCacheManager.getCache();
      String rv = rc.put("Key", "Value");
      assert rv == null;
      rv = rc.put("Key", "Value2");
      assert rv == null;
   }

   public void testForceReturnValues() {
      RemoteCache<String, String> rc = remoteCacheManager.getCache(true);
      String rv = rc.put("Key", "Value");
      assert rv == null;
      rv = rc.put("Key", "Value2");
      assert rv != null;
      assert "Value".equals(rv);
   }
}
