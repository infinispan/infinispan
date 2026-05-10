package org.infinispan.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Key;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "replication.AsyncAPITxSyncReplTest")
public class AsyncAPITxSyncReplTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfig();
      c.transaction().autoCommit(false);
      createClusteredCaches(2, TestDataSCI.INSTANCE, c);
   }

   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
   }

   protected void assertOnAllCaches(Key k, String v, Cache c1, Cache c2) {
      assertEquals(v, c1.get(k), "Error in cache 1.");
      assertEquals(v, c2.get(k), "Error in cache 2.");
   }

   public void testAsyncTxMethods() throws Exception {
      Cache<Object, String> c1 = cache(0);
      Cache c2 = cache(1);

      String v = "v";
      String v2 = "v2";
      String v3 = "v3";
      String v4 = "v4";
      String v5 = "v5";
      String v6 = "v6";
      String v_null = "v_nonexistent";
      Key key = new Key("k");
      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      // put
      tm.begin();
      Future<String> f = c1.putAsync(key, v);
      assertNotNull(f);
      Transaction t = tm.suspend();
      assertNull(c2.get(key));
      tm.resume(t);
      assertNull(f.get());
      tm.commit();
      assertOnAllCaches(key, v, c1, c2);

      tm.begin();
      f = c1.putAsync(key, v2);
      assertNotNull(f);
      t = tm.suspend();
      assertEquals(v, c2.get(key));
      tm.resume(t);
      assertFalse(f.isCancelled());
      assertEquals(v, f.get());
      tm.commit();
      assertOnAllCaches(key, v2, c1, c2);

      // putAll
      tm.begin();
      final Future<Void> f2 = c1.putAllAsync(Collections.singletonMap(key, v3));
      assertNotNull(f2);
      eventually(f2::isDone);
      t = tm.suspend();
      assertEquals(v2, c2.get(key));
      tm.resume(t);
      assertFalse(f2.isCancelled());
      assertNull(f2.get());
      tm.commit();
      assertOnAllCaches(key, v3, c1, c2);

      // putIfAbsent
      tm.begin();
      final Future f1 = c1.putIfAbsentAsync(key, v4);
      assertNotNull(f1);
      eventually(f1::isDone);
      t = tm.suspend();
      assertEquals(v3, c2.get(key));
      tm.resume(t);
      assertFalse(f1.isCancelled());
      assertEquals(v3, f1.get());
      tm.commit();
      assertOnAllCaches(key, v3, c1, c2);

      // remove
      tm.begin();
      final Future f3 = c1.removeAsync(key);
      assertNotNull(f3);
      eventually(f3::isDone);
      t = tm.suspend();
      assertEquals(v3, c2.get(key));
      tm.resume(t);
      assertFalse(f3.isCancelled());
      assertEquals(v3, f3.get());
      tm.commit();
      assertOnAllCaches(key, null, c1, c2);

      // putIfAbsent again
      tm.begin();
      final Future f4 = c1.putIfAbsentAsync(key, v4);
      assertNotNull(f4);
      eventually(f4::isDone);
      assertTrue(f4.isDone());
      t = tm.suspend();
      assertNull(c2.get(key));
      tm.resume(t);
      assertFalse(f4.isCancelled());
      assertNull(f4.get());
      tm.commit();
      assertOnAllCaches(key, v4, c1, c2);

      // removecond
      tm.begin();
      Future<Boolean> f5 = c1.removeAsync(key, v_null);
      assertNotNull(f5);
      assertFalse(f5.isCancelled());
      assertEquals(false, f5.get());
      assertTrue(f5.isDone());
      tm.commit();
      assertOnAllCaches(key, v4, c1, c2);

      tm.begin();
      final Future f6 = c1.removeAsync(key, v4);
      assertNotNull(f6);
      eventually(f6::isDone);
      assertTrue(f6.isDone());
      t = tm.suspend();
      assertEquals(v4, c2.get(key));
      tm.resume(t);
      assertFalse(f6.isCancelled());
      assertEquals(true, f6.get());
      tm.commit();
      assertOnAllCaches(key, null, c1, c2);

      // replace
      tm.begin();
      final Future f7 = c1.replaceAsync(key, v5);
      assertNotNull(f7);
      assertFalse(f7.isCancelled());
      assertNull(f7.get());
      assertTrue(f7.isDone());
      tm.commit();
      assertOnAllCaches(key, null, c1, c2);


      tm.begin();
      c1.put(key, v);
      tm.commit();

      tm.begin();
      final Future f8 = c1.replaceAsync(key, v5);
      assertNotNull(f8);
      eventually(f8::isDone);
      t = tm.suspend();
      assertEquals(v, c2.get(key));
      tm.resume(t);
      assertFalse(f8.isCancelled());
      assertEquals(v, f8.get());
      tm.commit();
      assertOnAllCaches(key, v5, c1, c2);

      //replace2
      tm.begin();
      final Future f9 = c1.replaceAsync(key, v_null, v6);
      assertNotNull(f9);
      assertFalse(f9.isCancelled());
      assertEquals(false, f9.get());
      assertTrue(f9.isDone());
      tm.commit();
      assertOnAllCaches(key, v5, c1, c2);

      tm.begin();
      final Future f10 = c1.replaceAsync(key, v5, v6);
      assertNotNull(f10);
      eventually(f10::isDone);
      t = tm.suspend();
      assertEquals(v5, c2.get(key));
      tm.resume(t);
      assertFalse(f10.isCancelled());
      assertEquals(true, f10.get());
      tm.commit();
      assertOnAllCaches(key, v6, c1, c2);
   }
}
