package org.infinispan.notifications.cachelistener;

import static org.easymock.EasyMock.*;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierTest")
public class CacheNotifierTest {
   private Cache<Object, Object> cache;
   private TransactionManager tm;
   private CacheNotifier mockNotifier;
   private CacheNotifier origNotifier;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.LOCAL);
      c.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());

      CacheManager cm = TestCacheManagerFactory.createCacheManager(c);

      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
      mockNotifier = createMock(CacheNotifier.class);
      origNotifier = TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws Exception {
      TestingUtil.replaceComponent(cache, CacheNotifier.class, origNotifier, true);
      TestingUtil.killCaches(cache);
   }

   @AfterTest
   public void destroyManager() {
      TestingUtil.killCacheManagers(cache.getCacheManager());
   }

   private void initCacheData(Object key, Object value) {
      initCacheData(Collections.singletonMap(key, value));
   }

   private void initCacheData(Map data) {
      mockNotifier.notifyCacheEntryCreated(anyObject(), anyBoolean(), isA(InvocationContext.class));
      expectLastCall().anyTimes();
      mockNotifier.notifyCacheEntryModified(anyObject(), anyObject(), anyBoolean(), isA(InvocationContext.class));
      expectLastCall().anyTimes();
      replay(mockNotifier);
      cache.putAll(data);
      verify(mockNotifier);

      // now reset the mock
      reset(mockNotifier);
   }

   private void expectSingleEntryCreated(Object key, Object value) {
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
      mockNotifier.notifyTransactionRegistered(isA(Transaction.class), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyTransactionCompleted(isA(Transaction.class), eq(successful), isA(InvocationContext.class));
      expectLastCall().once();
   }


   // simple tests first

   public void testCreation() throws Exception {
      expectSingleEntryCreated("key", "value");
      replay(mockNotifier);
      cache.put("key", "value");
      verify(mockNotifier);
   }

   public void testOnlyModification() throws Exception {
      initCacheData("key", "value");

      mockNotifier.notifyCacheEntryModified(eq("key"), eq("value"), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryModified(eq("key"), eq("value2"), eq(false), isA(InvocationContext.class));
      expectLastCall().once();

      replay(mockNotifier);
      cache.put("key", "value2");
      verify(mockNotifier);
   }

   public void testNonexistentRemove() throws Exception {
      cache.remove("doesNotExist");
      replay(mockNotifier);
      verify(mockNotifier);
   }

   public void testVisit() throws Exception {
      initCacheData("key", "value");

      mockNotifier.notifyCacheEntryVisited(eq("key"), eq(true), isA(InvocationContext.class));
      expectLastCall().once();
      mockNotifier.notifyCacheEntryVisited(eq("key"), eq(false), isA(InvocationContext.class));
      expectLastCall().once();
      replay(mockNotifier);
      cache.get("key");
      verify(mockNotifier);
   }

   public void testNonexistentVisit() throws Exception {
      cache.get("doesNotExist");
      replay(mockNotifier);
      verify(mockNotifier);
   }

   public void testRemoveData() throws Exception {
      Map data = new HashMap();
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
      Map data = new HashMap();
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
