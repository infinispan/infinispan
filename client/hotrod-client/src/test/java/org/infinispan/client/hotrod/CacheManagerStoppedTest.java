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

import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.infinispan.test.TestingUtil.*;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.CacheManagerStoppedTest", groups = "functional")
public class CacheManagerStoppedTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "someName";

   EmbeddedCacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(CACHE_NAME, hotRodCacheConfiguration().build());
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager("localhost:" + hotrodServer.getPort(), true);
      return cacheManager;
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killCacheManagers(cacheManager);
      killServers(hotrodServer);
   }

   public void testGetCacheOperations() {
      assert remoteCacheManager.getCache() != null;
      assert remoteCacheManager.getCache(CACHE_NAME) != null;
      remoteCache().put("k", "v");
      assert remoteCache().get("k").equals("v");
   }

   @Test (dependsOnMethods = "testGetCacheOperations")
   public void testStopCacheManager() {
      assert remoteCacheManager.isStarted();
      remoteCacheManager.stop();
      assert !remoteCacheManager.isStarted();
      assert remoteCacheManager.getCache() != null;
      assert remoteCacheManager.getCache(CACHE_NAME) != null;
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testGetCacheOperations2() {
      remoteCacheManager.getCache().put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testGetCacheOperations3() {
      remoteCacheManager.getCache(CACHE_NAME).put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPut() {
      remoteCache().put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAsync() {
      remoteCache().putAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testGet() {
      remoteCache().get("k");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testReplace() {
      remoteCache().replace("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testReplaceAsync() {
      remoteCache().replaceAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAll() {
      remoteCache().putAll(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAllAsync() {
      remoteCache().putAllAsync(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedGet() {
      remoteCache().getVersioned("key");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedRemove() {
      remoteCache().removeWithVersion("key", 12312321l);
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedRemoveAsync() {
      remoteCache().removeWithVersionAsync("key", 12312321l);
   }

   private RemoteCache<Object, Object> remoteCache() {
      return remoteCacheManager.getCache();
   }
}
