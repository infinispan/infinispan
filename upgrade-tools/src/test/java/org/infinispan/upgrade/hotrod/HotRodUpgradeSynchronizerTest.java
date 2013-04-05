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
package org.infinispan.upgrade.hotrod;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.CacheValue;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.ByteArrayKey;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(testName = "upgrade.hotrod.HotRodUpgradeSynchronizerTest", groups = "functional")
public class HotRodUpgradeSynchronizerTest extends AbstractInfinispanTest {

   private HotRodServer sourceServer;
   private HotRodServer targetServer;
   private EmbeddedCacheManager sourceContainer;
   private Cache<ByteArrayKey, CacheValue> sourceServerCache;
   private EmbeddedCacheManager targetContainer;
   private Cache<ByteArrayKey, CacheValue> targetServerCache;
   private RemoteCacheManager sourceRemoteCacheManager;
   private RemoteCache<String, String> sourceRemoteCache;
   private RemoteCacheManager targetRemoteCacheManager;
   private RemoteCache<String, String> targetRemoteCache;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      sourceContainer = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(serverBuilder));
      sourceServerCache = sourceContainer.getCache();
      sourceServer = TestHelper.startHotRodServer(sourceContainer);

      ConfigurationBuilder targetConfigurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      targetConfigurationBuilder.loaders().addStore(RemoteCacheStoreConfigurationBuilder.class).hotRodWrapping(true).addServer().host("localhost").port(sourceServer.getPort());

      targetContainer = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(targetConfigurationBuilder));
      targetServerCache = targetContainer.getCache();
      targetServer = TestHelper.startHotRodServer(targetContainer);

      sourceRemoteCacheManager = new RemoteCacheManager("localhost", sourceServer.getPort());
      sourceRemoteCacheManager.start();
      sourceRemoteCache = sourceRemoteCacheManager.getCache();

      targetRemoteCacheManager = new RemoteCacheManager("localhost", targetServer.getPort());
      targetRemoteCacheManager.start();
      targetRemoteCache = targetRemoteCacheManager.getCache();
   }

   public void testSynchronization() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s);
      }
      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteCache.get("A"));

      RollingUpgradeManager sourceUpgradeManager = sourceServerCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      sourceUpgradeManager.recordKnownGlobalKeyset();
      RollingUpgradeManager targetUpgradeManager = targetServerCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      targetUpgradeManager.synchronizeData("hotrod");
      // The server contains one extra key: MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS
      assertEquals(sourceServerCache.size() - 1, targetServerCache.size());

      targetUpgradeManager.disconnectSource("hotrod");
      CacheLoaderManager loaderManager = targetServerCache.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      assertFalse(loaderManager.isEnabled());
   }

   @BeforeMethod(alwaysRun = true)
   public void cleanup() {
      sourceServerCache.clear();
      targetServerCache.clear();
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManagers(sourceRemoteCacheManager, targetRemoteCacheManager);
      HotRodClientTestingUtil.killServers(sourceServer, targetServer);
      TestingUtil.killCacheManagers(targetContainer, sourceContainer);
   }

}
