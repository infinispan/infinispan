package org.infinispan.replication;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.Future;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Key;
import org.infinispan.test.eventually.Eventually;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.AsyncAPITxSyncReplTest")
public class AsyncAPITxSyncReplTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfig();
      c.transaction().autoCommit(false);
      createClusteredCaches(2, c);
   }

   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
   }

   protected void assertOnAllCaches(Key k, String v, Cache c1, Cache c2) {
      assertEquals("Error in cache 1.", v, c1.get(k));
      assertEquals("Error in cache 2,", v, c2.get(k));
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
      Key key = new Key("k", false);
      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      // put
      tm.begin();
      Future<String> f = c1.putAsync(key, v);
      assert f != null;
      Transaction t = tm.suspend();
      assert c2.get(key) == null;
      tm.resume(t);
      assert f.get() == null;
      tm.commit();
      assertOnAllCaches(key, v, c1, c2);

      tm.begin();
      f = c1.putAsync(key, v2);
      assert f != null;
      t = tm.suspend();
      assert c2.get(key).equals(v);
      tm.resume(t);
      assert !f.isCancelled();
      assert f.get().equals(v);
      tm.commit();
      assertOnAllCaches(key, v2, c1, c2);

      // putAll
      tm.begin();
      final Future<Void> f2 = c1.putAllAsync(Collections.singletonMap(key, v3));
      assert f2 != null;
      Eventually.eventually(f2::isDone);
      t = tm.suspend();
      assert c2.get(key).equals(v2);
      tm.resume(t);
      assert !f2.isCancelled();
      assert f2.get() == null;
      tm.commit();
      assertOnAllCaches(key, v3, c1, c2);

      // putIfAbsent
      tm.begin();
      final Future f1 = c1.putIfAbsentAsync(key, v4);
      assert f1 != null;
      Eventually.eventually(f1::isDone);
      t = tm.suspend();
      assert c2.get(key).equals(v3);
      tm.resume(t);
      assert !f1.isCancelled();
      assert f1.get().equals(v3);
      tm.commit();
      assertOnAllCaches(key, v3, c1, c2);

      // remove
      tm.begin();
      final Future f3 = c1.removeAsync(key);
      assert f3 != null;
      Eventually.eventually(f3::isDone);
      t = tm.suspend();
      assert c2.get(key).equals(v3);
      tm.resume(t);
      assert !f3.isCancelled();
      assert f3.get().equals(v3);
      tm.commit();
      assertOnAllCaches(key, null, c1, c2);

      // putIfAbsent again
      tm.begin();
      final Future f4 = c1.putIfAbsentAsync(key, v4);
      assert f4 != null;
      Eventually.eventually(f4::isDone);
      assert f4.isDone();
      t = tm.suspend();
      assert c2.get(key) == null;
      tm.resume(t);
      assert !f4.isCancelled();
      assert f4.get() == null;
      tm.commit();
      assertOnAllCaches(key, v4, c1, c2);

      // removecond
      tm.begin();
      Future<Boolean> f5 = c1.removeAsync(key, v_null);
      assert f5 != null;
      assert !f5.isCancelled();
      assert f5.get().equals(false);
      assert f5.isDone();
      tm.commit();
      assertOnAllCaches(key, v4, c1, c2);

      tm.begin();
      final Future f6 = c1.removeAsync(key, v4);
      assert f6 != null;
      Eventually.eventually(f6::isDone);
      assert f6.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v4);
      tm.resume(t);
      assert !f6.isCancelled();
      assert f6.get().equals(true);
      tm.commit();
      assertOnAllCaches(key, null, c1, c2);

      // replace
      tm.begin();
      final Future f7 = c1.replaceAsync(key, v5);
      assert f7 != null;
      assert !f7.isCancelled();
      assert f7.get() == null;
      assert f7.isDone();
      tm.commit();
      assertOnAllCaches(key, null, c1, c2);


      tm.begin();
      c1.put(key, v);
      tm.commit();

      tm.begin();
      final Future f8 = c1.replaceAsync(key, v5);
      assert f8 != null;
      Eventually.eventually(f8::isDone);
      t = tm.suspend();
      assert c2.get(key).equals(v);
      tm.resume(t);
      assert !f8.isCancelled();
      assert f8.get().equals(v);
      tm.commit();
      assertOnAllCaches(key, v5, c1, c2);

      //replace2
      tm.begin();
      final Future f9 = c1.replaceAsync(key, v_null, v6);
      assert f9 != null;
      assert !f9.isCancelled();
      assert f9.get().equals(false);
      assert f9.isDone();
      tm.commit();
      assertOnAllCaches(key, v5, c1, c2);

      tm.begin();
      final Future f10 = c1.replaceAsync(key, v5, v6);
      assert f10 != null;
      Eventually.eventually(f10::isDone);
      t = tm.suspend();
      assert c2.get(key).equals(v5);
      tm.resume(t);
      assert !f10.isCancelled();
      assert f10.get().equals(true);
      tm.commit();
      assertOnAllCaches(key, v6, c1, c2);
   }
}
