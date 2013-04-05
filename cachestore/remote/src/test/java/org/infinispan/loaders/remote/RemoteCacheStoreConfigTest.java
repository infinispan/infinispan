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

import org.infinispan.Cache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * Simple test to sample how remote cache store is configured.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "loaders.remote.RemoteCacheStoreConfigTest", groups = "functional")
public class RemoteCacheStoreConfigTest {

   public static final String CACHE_LOADER_CONFIG = "remote-cl-config.xml";
   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotRodServer;

   @BeforeTest
   public void startUp() {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      assertEquals(cacheManager.getCache().size(), 0);
      hotRodServer = HotRodTestingUtil.startHotRodServer(cacheManager, 19711);
   }

   public void simpleTest() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            CacheLoader cacheLoader = TestingUtil.getCacheLoader(cache);
            assert cacheLoader != null;
            assert cacheLoader instanceof RemoteCacheStore;


            cache.put("k", "v");

            assertEquals(1, cacheManager.getCache().size());
            cache.stop();
            assertEquals(1, cacheManager.getCache().size());
         }
      });

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG)) {
         @Override
         public void call() {
            Cache cache = cm.getCache();
            assertEquals("v", cache.get("k"));
         }
      });
   }

   @AfterTest(alwaysRun = true)
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hotRodServer);
      TestingUtil.killCacheManagers(cacheManager);
   }
}
