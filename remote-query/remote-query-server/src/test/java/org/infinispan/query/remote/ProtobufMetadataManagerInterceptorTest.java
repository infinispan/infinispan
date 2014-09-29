package org.infinispan.query.remote;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.query.remote.logging.Log;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@Test(groups = "functional", testName = "query.remote.ProtobufMetadataManagerInterceptorTest")
public class ProtobufMetadataManagerInterceptorTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(ProtobufMetadataManagerInterceptorTest.class, Log.class);

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
            .clustering().sync()
            .stateTransfer().fetchInMemoryState(true)
            .transaction().lockingMode(LockingMode.PESSIMISTIC).syncCommitPhase(true).syncRollbackPhase(true)
            .locking().isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false)
            .customInterceptors().addInterceptor()
            .interceptor(new ProtobufMetadataManagerInterceptor()).after(PessimisticLockingInterceptor.class);
      return cfg;
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
         assertEquals("The key must be a string", e.getMessage());
      }

      try {
         cache(0).put("some.proto", 42);
         fail();
      } catch (CacheException e) {
         assertEquals("The value must be a string", e.getMessage());
      }

      try {
         cache0.put("some.xml", "import \"test.proto\";");
         fail();
      } catch (CacheException e) {
         assertEquals("The key must end with \".proto\" : some.xml", e.getMessage());
      }

      try {
         cache0.put("test.proto", "package x");
         fail();
      } catch (CacheException e) {
         assertEquals("Failed to parse proto file : test.proto", e.getMessage());
      }

      try {
         Map<String, String> map = new HashMap<>();
         map.put("a.proto", "package a");
         map.put("b.proto", "package b;");
         cache0.putAll(map);
         fail();
      } catch (CacheException e) {
         // todo [anistor] the error message is very ugly here ..
         assertTrue(e.getCause() instanceof DescriptorParserException);
         assertTrue(e.getMessage().contains("Syntax error in a.proto"));
      }

      assertTrue(cache0.isEmpty());
      assertTrue(cache1.isEmpty());

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

      try {
         cache0.replace("test.proto", "package XYX");
         fail();
      } catch (CacheException e) {
         // todo [anistor] the error message is very ugly here ..
         assertTrue(e.getCause() instanceof DescriptorParserException);
         assertTrue(e.getMessage().contains("Failed to parse proto file : test.proto"));
      }

      assertEquals(1, cache0.size());
      assertEquals(1, cache1.size());
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));

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
      assertEquals("test.proto", cache0.get(".errors"));
      assertEquals("Import 'missing.proto' not found", cache1.get("test.proto.errors"));
      assertEquals("test.proto", cache1.get(".errors"));

      value = "package foobar;";
      cache0.put("test.proto", value);
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertFalse(cache0.containsKey("test.proto.errors"));
      assertFalse(cache1.containsKey("test.proto.errors"));
      assertFalse(cache0.containsKey(".errors"));
      assertFalse(cache1.containsKey(".errors"));

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
      assertEquals("test.proto", cache0.get(".errors"));
      assertEquals("Import 'missing.proto' not found", cache1.get("test.proto.errors"));
      assertEquals("test.proto", cache1.get(".errors"));

      cache0.putIfAbsent("test.proto", "package foobar;");
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertEquals("Import 'missing.proto' not found", cache0.get("test.proto.errors"));
      assertEquals("test.proto", cache0.get(".errors"));
      assertEquals("Import 'missing.proto' not found", cache1.get("test.proto.errors"));
      assertEquals("test.proto", cache1.get(".errors"));

      value = "package foobar;";
      cache0.put("test.proto", value);
      assertEquals(value, cache0.get("test.proto"));
      assertEquals(value, cache1.get("test.proto"));
      assertFalse(cache0.containsKey("test.proto.errors"));
      assertFalse(cache1.containsKey("test.proto.errors"));
      assertFalse(cache0.containsKey(".errors"));
      assertFalse(cache1.containsKey(".errors"));

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
      assertTrue(cache0.containsKey(".errors"));
      assertTrue(cache1.containsKey(".errors"));
      assertEquals("a.proto", cache0.get(".errors"));
      assertEquals("a.proto", cache1.get(".errors"));

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
      assertFalse(cache0.containsKey(".errors"));
      assertFalse(cache1.containsKey(".errors"));

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
      assertFalse(cache0.containsKey(".errors"));
      assertFalse(cache1.containsKey(".errors"));

      assertNoTransactionsAndLocks();
   }

   private void assertNoTransactionsAndLocks() {
      assertNoTransactions();
      TestingUtil.assertNoLocks(cache(0));
      TestingUtil.assertNoLocks(cache(1));
   }
}
