/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.client.hotrod;

import org.testng.annotations.Test;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.*;

/**
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test (testName = "client.hotrod.DefaultExpirationTest", groups = "functional" )
public class DefaultExpirationTest extends SingleCacheManagerTest {
   private RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      builder.expiration().lifespan(3, TimeUnit.SECONDS).maxIdle(2, TimeUnit.SECONDS);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      //pass the config file to the cache
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());
      remoteCacheManager = getRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache();
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      config.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      return new RemoteCacheManager(config);
   }


   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
   }

   @Test
   public void testDefaultExpiration() throws Exception {
      remoteCache.put("Key", "Value");
      InternalCacheEntry entry = assertHotRodEquals(cacheManager, "Key", "Value");
      assertTrue(entry.canExpire());
      assertEquals(3000, entry.getLifespan());
      assertEquals(2000, entry.getMaxIdle());
      Thread.sleep(5000);
      assertFalse(remoteCache.containsKey("Key"));
   }

}
