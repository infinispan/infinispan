/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierTest")
public class CacheNotifierTest extends AbstractInfinispanTest {

   private Cache<Object, Object> cache;
   private CacheNotifier mockNotifier;
   private CacheNotifier origNotifier;
   private CacheContainer cm;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      Configuration c = new Configuration();
      c.fluent().transaction().transactionalCache(false);
      c.setCacheMode(Configuration.CacheMode.LOCAL);
      c.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      cm = TestCacheManagerFactory.createCacheManager(c);

      cache = cm.getCache();
      mockNotifier = createMock(CacheNotifier.class);
      origNotifier = TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws Exception {
      TestingUtil.replaceComponent(cache, CacheNotifier.class, origNotifier, true);
      TestingUtil.killCaches(cache);
      cm.stop();
   }

   @AfterClass
   public void destroyManager() {
      TestingUtil.killCacheManagers(cache.getCacheManager());
   }

   public void testVisit() throws Exception {
      initCacheData(Collections.singletonMap("key", "value"));

      mockNotifier.notifyCacheEntryVisited(eq("key"), eq("value"), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryVisited(eq("key"), eq("value"), eq(false), isA(InvocationContext.class));
      expectLastCall().once();
      replay(mockNotifier);
      cache.get("key");
      verify(mockNotifier);
   }


   public void testRemoveData() throws Exception {
      Map<String, String> data = new HashMap<String, String>();
      data.put("key", "value");
      data.put("key2", "value2");
      initCacheData(data);

      mockNotifier.notifyCacheEntryRemoved(eq("key2"), eq("value2"), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryRemoved(eq("key2"), isNull(), eq(false), isA(InvocationContext.class));
      expectLastCall().once();
      replay(mockNotifier);

      cache.remove("key2");

      verify(mockNotifier);
   }

   public void testPutMap() throws Exception {
      Map<Object, Object> data = new HashMap<Object, Object>();
      data.put("key", "value");
      data.put("key2", "value2");
      expectSingleEntryCreated("key", "value");
      expectSingleEntryCreated("key2", "value2");

      replay(mockNotifier);

      cache.putAll(data);
      verify(mockNotifier);
   }

   public void testOnlyModification() throws Exception {
      initCacheData(Collections.singletonMap("key", "value"));

      mockNotifier.notifyCacheEntryModified(eq("key"), eq("value"), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryModified(eq("key"), eq("value2"), eq(false), isA(InvocationContext.class));
      expectLastCall().once();

      replay(mockNotifier);
      cache.put("key", "value2");
      verify(mockNotifier);
   }

   public void testNonexistentVisit() throws Exception {
      cache.get("doesNotExist");
      replay(mockNotifier);
      verify(mockNotifier);
   }

   public void testNonexistentRemove() throws Exception {
      cache.remove("doesNotExist");
      replay(mockNotifier);
      verify(mockNotifier);
   }

   public void testCreation() throws Exception {
      expectSingleEntryCreated("key", "value");
      replay(mockNotifier);
      cache.put("key", "value");
      verify(mockNotifier);
   }

   private void initCacheData(Map<String, String> data) {
      mockNotifier.notifyCacheEntryCreated(anyObject(), anyBoolean(), isA(InvocationContext.class));
      expectLastCall().anyTimes();
      mockNotifier.notifyCacheEntryModified(anyObject(), anyObject(), anyBoolean(), isA(InvocationContext.class));
      expectLastCall().anyTimes();
      replay(mockNotifier);
         cache.putAll(data);
      verify(mockNotifier);

      // now resume the mock
      reset(mockNotifier);
   }

   private void expectSingleEntryCreated(Object key, Object value) {
      CacheNotifierTxTest.expectSingleEntryCreated(key, value, this.mockNotifier);
   }
}
