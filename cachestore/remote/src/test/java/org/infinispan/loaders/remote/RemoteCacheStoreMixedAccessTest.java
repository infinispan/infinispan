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

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
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

@Test(testName = "loaders.remote.RemoteCacheStoreMixedAccessTest", groups="functional")
public class RemoteCacheStoreMixedAccessTest extends AbstractInfinispanTest {

   private HotRodServer hrServer;
   private EmbeddedCacheManager serverCacheManager;
   private Cache<String, String> serverCache;
   private EmbeddedCacheManager clientCacheManager;
   private Cache<String, String> clientCache;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<String, String> remoteCache;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      serverBuilder.eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED)
            .expiration().wakeUpInterval(10L);
      serverCacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(serverBuilder));
      serverCache = serverCacheManager.getCache();
      hrServer = TestHelper.startHotRodServer(serverCacheManager);

      ConfigurationBuilder clientBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      clientBuilder.loaders().addStore(RemoteCacheStoreConfigurationBuilder.class)
         .rawValues(true)
         .addServer()
            .host("localhost")
            .port(hrServer.getPort());
      clientCacheManager = TestCacheManagerFactory.createCacheManager(clientBuilder);
      clientCache = clientCacheManager.getCache();
      remoteCacheManager = new RemoteCacheManager("localhost", hrServer.getPort());
      remoteCacheManager.start();
      remoteCache = remoteCacheManager.getCache();
   }

   public void testMixedAccess() {
      remoteCache.put("k1", "v1");
      String rv1 = remoteCache.get("k1");
      assertEquals("v1", rv1);
      MetadataValue<String> mv1 = remoteCache.getWithMetadata("k1");
      assertEquals("v1", mv1.getValue());
      String cv1 = clientCache.get("k1");
      assertEquals("v1", cv1);
   }

   public void testMixedAccessWithLifespan() {
      remoteCache.put("k1", "v1", 120, TimeUnit.SECONDS);
      MetadataValue<String> mv1 = remoteCache.getWithMetadata("k1");
      assertEquals("v1", mv1.getValue());
      assertEquals(120, mv1.getLifespan());
      String cv1 = clientCache.get("k1");
      assertEquals("v1", cv1);
      InternalCacheEntry ice1 = clientCache.getAdvancedCache().getDataContainer().get("k1");
      assertEquals(120000, ice1.getLifespan());
   }

   public void testMixedAccessWithLifespanAndMaxIdle() {
      remoteCache.put("k1", "v1", 120, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      MetadataValue<String> mv1 = remoteCache.getWithMetadata("k1");
      assertEquals("v1", mv1.getValue());
      assertEquals(120, mv1.getLifespan());
      assertEquals(30, mv1.getMaxIdle());
      String cv1 = clientCache.get("k1");
      assertEquals("v1", cv1);
      InternalCacheEntry ice1 = clientCache.getAdvancedCache().getDataContainer().get("k1");
      assertEquals(120000, ice1.getLifespan());
      assertEquals(30000, ice1.getMaxIdle());
   }

   @BeforeMethod
   public void cleanup() {
      serverCache.clear();
      clientCache.clear();
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(clientCacheManager, serverCacheManager);
   }

}
