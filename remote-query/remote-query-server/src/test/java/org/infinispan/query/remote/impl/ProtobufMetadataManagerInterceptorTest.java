package org.infinispan.query.remote.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.remote.impl.ProtobufMetadataManagerInterceptorTest")
public class ProtobufMetadataManagerInterceptorTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      //todo [anistor] test with ST, with store , test with manual TX, with batching, with HotRod
      addClusterEnabledCacheManager(makeCfg());
      addClusterEnabledCacheManager(makeCfg());
      waitForClusterToForm();
   }

   private ConfigurationBuilder makeCfg() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL).invocationBatching().enable()
            .clustering().cacheMode(CacheMode.REPL_SYNC)
            .clustering()
            .stateTransfer().fetchInMemoryState(true)
            .transaction().lockingMode(LockingMode.PESSIMISTIC)
            .locking().isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false)
            .customInterceptors().addInterceptor()
            .interceptor(new ProtobufMetadataManagerInterceptor()).after(PessimisticLockingInterceptor.class);
      return cfg;
   }

   @AfterMethod
   @Override
   protected void clearContent() {
      // the base method cleans only the data container without invoking the interceptor stack...
      cache(0).clear();
   }

   public void testValidatePut() {
      Cache<String, String> cache0 = cache(0);
      Cache<String, String> cache1 = cache(1);

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());

      try {
         cache(0).put(42, "import \"test.proto\";");
         fail();
      } catch (CacheException e) {
         assertEquals("ISPN028007: The key must be a String : class java.lang.Integer", e.getMessage());
      }

      try {
         cache(0).put("some.proto", 42);
         fail();
      } catch (CacheException e) {
         assertEquals("ISPN028008: The value must be a String : class java.lang.Integer", e.getMessage());
      }

      try {
         cache0.put("some.xml", "import \"test.proto\";");
         fail();
      } catch (CacheException e) {
         assertEquals("ISPN028009: The key must be a String ending with \".proto\" : some.xml", e.getMessage());
      }

      cache0.put("test.proto", "package x");
      assertEquals("test.proto", cache0.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("test.proto", cache1.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      cache0.remove("test.proto");

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());

      Map<String, String> map = new HashMap<>();
      map.put("a.proto", "package a");
      map.put("b.proto", "package b;");
      cache0.putAll(map);
      assertEquals("a.proto", cache0.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("a.proto", cache1.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      assertEquals(4, cache0.size());
      assertEquals(4, cache1.size());

      assertNoTransactionsAndLocks();
   }

   public void testValidateReplace() {
      Cache<String, String> cache0 = cache(0);
      Cache<String, String> cache1 = cache(1);

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());
      String value = "package X;";
      cache0.put("test.proto", value);

      assertEquals(1, cache0.size());
      assertEquals(1, cache1.size());
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));

      String newValue = "package XYX";
      cache0.replace("test.proto", newValue);
      assertEquals("test.proto", cache0.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("test.proto", cache1.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      assertEquals(3, cache0.size());
      assertEquals(3, cache1.size());
      assertEquals(newValue, cache0.get("test.proto"));
      assertEquals(newValue, cache1.get("test.proto"));

      assertNoTransactionsAndLocks();
   }

   public void testStatusAfterPut() {
      Cache<String, String> cache0 = cache(0);
      Cache<String, String> cache1 = cache(1);

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());

      String value = "import \"missing.proto\";";
      cache0.put("test.proto", value);

      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertEquals("Import 'missing.proto' not found", cache0.get("test.proto.errors"));
      assertEquals("test.proto", cache0.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("Import 'missing.proto' not found", cache1.get("test.proto.errors"));
      assertEquals("test.proto", cache1.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      value = "package foobar;";
      cache0.put("test.proto", value);
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertFalse(cache0.containsKey("test.proto.errors"));
      assertFalse(cache1.containsKey("test.proto.errors"));
      assertFalse(cache0.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertFalse(cache1.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      assertNoTransactionsAndLocks();
   }

   public void testStatusAfterPutIfAbsent() {
      Cache<String, String> cache0 = cache(0);
      Cache<String, String> cache1 = cache(1);

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());

      String value = "import \"missing.proto\";";
      cache0.putIfAbsent("test.proto", value);

      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertEquals("Import 'missing.proto' not found", cache0.get("test.proto.errors"));
      assertEquals("test.proto", cache0.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("Import 'missing.proto' not found", cache1.get("test.proto.errors"));
      assertEquals("test.proto", cache1.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      cache0.putIfAbsent("test.proto", "package foobar;");
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertEquals("Import 'missing.proto' not found", cache0.get("test.proto.errors"));
      assertEquals("test.proto", cache0.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("Import 'missing.proto' not found", cache1.get("test.proto.errors"));
      assertEquals("test.proto", cache1.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      value = "package foobar;";
      cache0.put("test.proto", value);
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertFalse(cache0.containsKey("test.proto.errors"));
      assertFalse(cache1.containsKey("test.proto.errors"));
      assertFalse(cache0.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertFalse(cache1.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      assertNoTransactionsAndLocks();
   }

   public void testStatusAfterPutAll() {
      Cache<String, String> cache0 = cache(0);
      Cache<String, String> cache1 = cache(1);

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());

      String file1 = "import \"missing.proto\";";
      String file2 = "package b;";
      Map<String, String> map = new HashMap<>();
      map.put("a.proto", file1);
      map.put("b.proto", file2);
      cache0.putAll(map);

      assertEquals(file1, cache0.get("a.proto"));
      assertEquals(file2, cache0.get("b.proto"));
      assertEquals(file1, cache1.get("a.proto"));
      assertEquals(file2, cache1.get("b.proto"));
      assertEquals("Import 'missing.proto' not found", cache0.get("a.proto.errors"));
      assertEquals("Import 'missing.proto' not found", cache1.get("a.proto.errors"));
      assertFalse(cache0.containsKey("b.proto.errors"));
      assertFalse(cache1.containsKey("b.proto.errors"));
      assertTrue(cache0.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertTrue(cache1.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("a.proto", cache0.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertEquals("a.proto", cache1.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      file1 = "package a;";
      map.put("a.proto", file1);
      cache0.putAll(map);

      assertEquals(file1, cache0.get("a.proto"));
      assertEquals(file1, cache1.get("a.proto"));
      assertEquals(file2, cache0.get("b.proto"));
      assertEquals(file2, cache1.get("b.proto"));
      assertFalse(cache0.containsKey("a.proto.errors"));
      assertFalse(cache1.containsKey("a.proto.errors"));
      assertFalse(cache0.containsKey("b.proto.errors"));
      assertFalse(cache1.containsKey("b.proto.errors"));
      assertFalse(cache0.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertFalse(cache1.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      assertNoTransactionsAndLocks();
   }

   public void testStatusAfterRemove() {
      Cache<String, String> cache0 = cache(0);
      Cache<String, String> cache1 = cache(1);

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());

      String value = "import \"missing.proto\";";
      cache0.put("test.proto", value);

      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertEquals("Import 'missing.proto' not found", cache0.get("test.proto.errors"));
      assertEquals("Import 'missing.proto' not found", cache1.get("test.proto.errors"));

      cache0.remove("test.proto");
      assertFalse(cache0.containsKey("test.proto"));
      assertFalse(cache1.containsKey("test.proto"));
      assertFalse(cache0.containsKey("test.proto.errors"));
      assertFalse(cache1.containsKey("test.proto.errors"));
      assertFalse(cache0.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertFalse(cache1.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      assertNoTransactionsAndLocks();
   }

   public void testUnsupportedCommands() {
      Cache<String, String> cache0 = cache(0);

      assertTrue(cache0.isEmpty());

      try {
         cache0.compute("test.proto", (k, v) -> "import \"missing.proto\";");
      } catch (Exception e) {
         assertTrue(e instanceof CacheException);
         assertTrue(e.getMessage().contains("ISPN028014"));
      }

      assertTrue(cache0.isEmpty());

      try {
         FunctionalMap.ReadWriteMap<String, String> rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(cache0.getAdvancedCache()));

         rwMap.eval("test.proto", "val", (value, view) -> {
            view.set(value);
            return "ret";
         }).join();

      } catch (CompletionException e) {
         assertTrue(e.getCause() instanceof CacheException);
         assertTrue(e.getCause().getMessage().contains("ISPN028014"));
      }

      assertTrue(cache0.isEmpty());

      assertNoTransactionsAndLocks();
   }

   /**
    * State transfer is interesting because StateConsumerImpl uses PutKeyValueCommand with InternalCacheEntry values,
    * in order to preserve timestamps.
    */
   public void testStateTransfer() {
      cache(0).put("state.proto", "import \"test.proto\";");

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(makeCfg());
      try {
         Cache<Object, Object> cache = manager.getCache();
         assertEquals("import \"test.proto\";", cache.get("state.proto"));
         cache(0).remove("state.proto");
      } finally {
         killMember(2);
      }
   }

   private void assertNoTransactionsAndLocks() {
      assertNoTransactions();
      TestingUtil.assertNoLocks(cache(0));
      TestingUtil.assertNoLocks(cache(1));
   }
}
