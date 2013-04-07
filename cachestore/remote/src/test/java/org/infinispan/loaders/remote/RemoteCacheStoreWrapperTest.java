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
package org.infinispan.loaders.remote;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "loaders.remote.RemoteCacheStoreWrapperTest", groups="functional")
public class RemoteCacheStoreWrapperTest extends AbstractInfinispanTest {

   private HotRodServer sourceServer;
   private HotRodServer targetServer;
   private EmbeddedCacheManager serverCacheManager;
   private Cache<byte[], byte[]> serverCache;
   private EmbeddedCacheManager targetCacheManager;
   private Cache<byte[], byte[]> targetCache;
   private RemoteCacheManager remoteSourceCacheManager;
   private RemoteCache<String, String> remoteSourceCache;
   private RemoteCacheManager remoteTargetCacheManager;
   private RemoteCache<String, String> remoteTargetCache;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      serverBuilder.eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED)
            .expiration().wakeUpInterval(10L);
      serverCacheManager = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(serverBuilder));
      serverCache = serverCacheManager.getCache();
      sourceServer = TestHelper.startHotRodServer(serverCacheManager);

      remoteSourceCacheManager = new RemoteCacheManager("localhost", sourceServer.getPort());
      remoteSourceCacheManager.start();
      remoteSourceCache = remoteSourceCacheManager.getCache();

      ConfigurationBuilder clientBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      clientBuilder.loaders().addStore(RemoteCacheStoreConfigurationBuilder.class)
         .hotRodWrapping(true)
         .addServer()
            .host("localhost")
            .port(sourceServer.getPort());
      targetCacheManager = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(clientBuilder));
      targetCache = targetCacheManager.getCache();
      targetServer = TestHelper.startHotRodServer(targetCacheManager);

      remoteTargetCacheManager = new RemoteCacheManager("localhost", targetServer.getPort());
      remoteTargetCacheManager.start();
      remoteTargetCache = remoteTargetCacheManager.getCache();
   }

   public void testEntryWrapping() throws Exception {
      remoteSourceCache.put("k1", "v1");
      remoteSourceCache.put("k2", "v2");
      assertHotRodEquals(targetCacheManager, "k1", "v1");
      String v1 = remoteTargetCache.get("k1");
      assertEquals("v1", v1);
      String v2 = remoteTargetCache.get("k2");
      assertEquals("v2", v2);
   }

   @BeforeMethod(alwaysRun=true)
   public void cleanup() {
      serverCache.clear();
      targetCache.clear();
   }

   @AfterClass(alwaysRun=true)
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManagers(remoteSourceCacheManager, remoteTargetCacheManager);
      HotRodClientTestingUtil.killServers(sourceServer, targetServer);
      TestingUtil.killCacheManagers(targetCacheManager, serverCacheManager);
   }

}
