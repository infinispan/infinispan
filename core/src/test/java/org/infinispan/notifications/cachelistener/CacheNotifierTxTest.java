package org.infinispan.notifications.cachelistener;

import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.CacheContainer;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierTxTest")
public class CacheNotifierTxTest extends AbstractInfinispanTest {
   private Cache<Object, Object> cache;
   private TransactionManager tm;
   private CacheContainer cm;

   @BeforeMethod
   public void setUp() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.transaction().autoCommit(false)
             .clustering().cacheMode(CacheMode.LOCAL)
             .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);

      cm = TestCacheManagerFactory.createCacheManager(builder);

      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
      CacheNotifier mockNotifier = mock(CacheNotifier.class, i -> CompletableFutures.completedNull());
      doReturn(true).when(mockNotifier).hasListener(any());
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   @AfterMethod
   public void tearDown() throws Exception {
      TestingUtil.killCaches(cache);
      cm.stop();
   }

   @AfterClass
   public void destroyManager() {
      TestingUtil.killCacheManagers(cache.getCacheManager());
   }

   private CacheNotifier getMockNotifier(Cache cache) {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(CacheNotifier.class);
   }

   private void initCacheData(Object key, Object value) {
      initCacheData(Collections.singletonMap(key, value));
   }

   private void initCacheData(Map<Object, Object> data) {
      try {
         tm.begin();
         cache.putAll(data);
         tm.commit();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }

      expectTransactionBoundaries(true);
      verify(getMockNotifier(cache), atLeastOnce()).notifyCacheEntryCreated(notNull(),
            notNull(), any(Metadata.class), anyBoolean(),
            any(InvocationContext.class), any(PutMapCommand.class));
      clearInvocations(getMockNotifier(cache));
   }

   private void expectSingleEntryCreated(Object key, Object value) {
      expectSingleEntryCreated(key, value, getMockNotifier(cache));
   }

   private void expectSingleEntryOnlyPreCreated(Object key, Object value) {
      expectSingleEntryOnlyPreCreated(key, value, getMockNotifier(cache));
   }

   static void expectSingleEntryCreated(Object key, Object value, CacheNotifier mockNotifier) {
      verify(mockNotifier).notifyCacheEntryCreated(eq(key), eq(value), any(Metadata.class),
            eq(true), isA(InvocationContext.class), isA(PutKeyValueCommand.class));
      verify(mockNotifier).notifyCacheEntryCreated(eq(key), eq(value), any(Metadata.class),
            eq(false), isA(InvocationContext.class), isNull());
   }

   static void expectSingleEntryOnlyPreCreated(Object key, Object value, CacheNotifier mockNotifier) {
      verify(mockNotifier).notifyCacheEntryCreated(eq(key), eq(value), any(Metadata.class),
            eq(true), isA(InvocationContext.class), isA(PutKeyValueCommand.class));
   }

   private void expectTransactionBoundaries(boolean successful) {
      verify(getMockNotifier(cache)).notifyTransactionRegistered(isA(GlobalTransaction.class), anyBoolean());
      verify(getMockNotifier(cache)).notifyTransactionCompleted(isA(GlobalTransaction.class), eq(successful), isA(InvocationContext.class));
   }

   // -- now the transactional ones

   public void testTxNonexistentRemove() throws Exception {
      tm.begin();
      cache.remove("doesNotExist");
      tm.commit();

      expectTransactionBoundaries(true);
   }

   public void testTxCreationCommit() throws Exception {
      tm.begin();
      cache.put("key", "value");
      tm.commit();

      expectTransactionBoundaries(true);
      expectSingleEntryCreated("key", "value");
   }

   public void testTxCreationRollback() throws Exception {
      tm.begin();
      cache.put("key", "value");
      tm.rollback();

      expectTransactionBoundaries(false);
      expectSingleEntryOnlyPreCreated("key", "value");
   }

   public void testTxOnlyModification() throws Exception {
      initCacheData("key", "value");
      tm.begin();
      cache.put("key", "value2");
      tm.commit();

      expectTransactionBoundaries(true);
      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value"),
            any(Metadata.class), eq(true), isA(InvocationContext.class), isA(PutKeyValueCommand.class));
      verify(getMockNotifier(cache)).notifyCacheEntryModified(eq("key"), eq("value2"), any(Metadata.class), eq("value"),
            any(Metadata.class), eq(false), isA(InvocationContext.class), isNull());
   }

   public void testTxRemoveData() throws Exception {
      Map<Object, Object> data = new HashMap<>();
      data.put("key", "value");
      data.put("key2", "value2");
      initCacheData(data);

      tm.begin();
      cache.remove("key2");
      tm.commit();

      expectTransactionBoundaries(true);
      verify(getMockNotifier(cache)).notifyCacheEntryRemoved(eq("key2"), eq("value2"), any(Metadata.class),
            eq(true), isA(InvocationContext.class), isA(RemoveCommand.class));
      verify(getMockNotifier(cache)).notifyCacheEntryRemoved(eq("key2"), eq("value2"), any(Metadata.class),
            eq(false), isA(InvocationContext.class), isNull());
   }
}
