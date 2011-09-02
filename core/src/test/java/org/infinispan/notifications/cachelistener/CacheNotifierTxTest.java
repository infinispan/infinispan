/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

import static org.easymock.EasyMock.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierTxTest")
public class CacheNotifierTxTest extends AbstractInfinispanTest {
   private Cache<Object, Object> cache;
   private TransactionManager tm;
   private CacheNotifier mockNotifier;
   private CacheNotifier origNotifier;
   private CacheContainer cm;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      Configuration c = new Configuration();
      c.fluent().transaction().autoCommit(false);
      c.setCacheMode(Configuration.CacheMode.LOCAL);
      c.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      cm = TestCacheManagerFactory.createCacheManager(c, true);

      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
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

   private void initCacheData(Object key, Object value) {
      initCacheData(Collections.singletonMap(key, value));
   }

   private void initCacheData(Map<Object, Object> data) {
      expectTransactionBoundaries(true);
      mockNotifier.notifyCacheEntryCreated(anyObject(), anyBoolean(), isA(InvocationContext.class));
      expectLastCall().anyTimes();
      mockNotifier.notifyCacheEntryModified(anyObject(), anyObject(), anyBoolean(), isA(InvocationContext.class));
      expectLastCall().anyTimes();
      replay(mockNotifier);
      try {
         tm.begin();
         cache.putAll(data);
         tm.commit();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      verify(mockNotifier);

      // now resume the mock
      reset(mockNotifier);
   }

   private void expectSingleEntryCreated(Object key, Object value) {
      expectSingleEntryCreated(key, value, this.mockNotifier);
   }

   static void expectSingleEntryCreated(Object key, Object value, CacheNotifier mockNotifier) {
      mockNotifier.notifyCacheEntryCreated(eq(key), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryCreated(eq(key), eq(false), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryModified(eq(key), isNull(), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryModified(eq(key), eq(value), eq(false), isA(InvocationContext.class));
      expectLastCall().once();
   }

   private void expectTransactionBoundaries(boolean successful) {
      mockNotifier.notifyTransactionRegistered(isA(GlobalTransaction.class), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyTransactionCompleted(isA(GlobalTransaction.class), eq(successful), isA(InvocationContext.class));
      expectLastCall().once();
   }

   // -- now the transactional ones

   public void testTxNonexistentRemove() throws Exception {
      expectTransactionBoundaries(true);
      replay(mockNotifier);
      tm.begin();
      cache.remove("doesNotExist");
      tm.commit();
      verify(mockNotifier);
   }

   public void testTxCreationCommit() throws Exception {
      expectTransactionBoundaries(true);
      expectSingleEntryCreated("key", "value");
      replay(mockNotifier);
      tm.begin();
      cache.put("key", "value");
      tm.commit();
      verify(mockNotifier);
   }

   public void testTxCreationRollback() throws Exception {
      expectTransactionBoundaries(false);
      expectSingleEntryCreated("key", "value");
      replay(mockNotifier);
      tm.begin();
      cache.put("key", "value");
      tm.rollback();
      verify(mockNotifier);
   }

   public void testTxOnlyModification() throws Exception {
      initCacheData("key", "value");
      expectTransactionBoundaries(true);
      mockNotifier.notifyCacheEntryModified(eq("key"), eq("value"), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryModified(eq("key"), eq("value2"), eq(false), isA(InvocationContext.class));
      expectLastCall().once();

      replay(mockNotifier);

      tm.begin();
      cache.put("key", "value2");
      tm.commit();

      verify(mockNotifier);
   }

   public void testTxRemoveData() throws Exception {
      Map<Object, Object> data = new HashMap<Object, Object>();
      data.put("key", "value");
      data.put("key2", "value2");
      initCacheData(data);
      expectTransactionBoundaries(true);
      mockNotifier.notifyCacheEntryRemoved(eq("key2"), eq("value2"), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryRemoved(eq("key2"), isNull(), eq(false), isA(InvocationContext.class));
      expectLastCall().once();
      replay(mockNotifier);

      tm.begin();
      cache.remove("key2");
      tm.commit();

      verify(mockNotifier);
   }
}
