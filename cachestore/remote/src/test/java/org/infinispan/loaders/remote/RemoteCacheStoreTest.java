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
package org.infinispan.loaders.remote;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "loaders.remote.RemoteCacheStoreTest", groups = "functional")
public class RemoteCacheStoreTest extends BaseCacheStoreTest {

   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   protected CacheStore createCacheStore() throws Exception {
      RemoteCacheStoreConfig remoteCacheStoreConfig = new RemoteCacheStoreConfig();
      remoteCacheStoreConfig.setUseDefaultRemoteCache(true);
      assert remoteCacheStoreConfig.isUseDefaultRemoteCache();

      localCacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      localCacheManager.getDefaultConfiguration().fluent()
         .eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED)
         .expiration().wakeUpInterval(10L)
         .build();
      hrServer = TestHelper.startHotRodServer(localCacheManager);
      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", "localhost:" + hrServer.getPort());
      remoteCacheStoreConfig.setHotRodClientProperties(properties);

      RemoteCacheStore remoteCacheStore = new RemoteCacheStore();
      remoteCacheStore.init(remoteCacheStoreConfig, getCache(), getMarshaller());
      remoteCacheStore.start();
      return remoteCacheStore;
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      hrServer.stop();
      localCacheManager.stop();
   }

   @Override
   protected void assertEventuallyExpires(String key) throws Exception {
      for (int i = 0; i < 10; i++) {
         if (cs.load("k") == null) break;
         Thread.sleep(1000);
      }
      assert cs.load("k") == null;
   }

   @Override
   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(3000);
   }

   @Override
   protected void purgeExpired() throws CacheLoaderException {
      localCacheManager.getCache().getAdvancedCache().getEvictionManager().processEviction();
   }

   /**
    * This is not supported, see assertion in {@link RemoteCacheStore#loadAllKeys(java.util.Set)}
    */
   @Override
   public void testLoadKeys() throws CacheLoaderException {
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", 100));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      TestingUtil.sleepThread(1100);
      assert null == cs.load("k1");
      cs.store(TestInternalCacheEntryFactory.create("k1", "v2", 100));
      assert cs.load("k1").getValue().equals("v2");
   }
}


