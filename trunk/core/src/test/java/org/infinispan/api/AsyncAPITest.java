package org.infinispan.api;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.AsyncAPITest")
public class AsyncAPITest extends SingleCacheManagerTest {
   Cache<String, String> c;

   protected CacheManager createCacheManager() throws Exception {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      c = cm.getCache();
      return cm;
   }
  
   public void testAsyncMethods() throws ExecutionException, InterruptedException {
      // put
      Future<String> f = c.putAsync("k", "v");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k").equals("v");

      f = c.putAsync("k", "v2");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert c.get("k").equals("v2");

      // putAll
      Future<Void> f2 = c.putAllAsync(Collections.singletonMap("k", "v3"));
      assert f2 != null;
      assert f2.isDone();
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert c.get("k").equals("v3");

      // putIfAbsent
      f = c.putIfAbsentAsync("k", "v4");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v3");
      assert c.get("k").equals("v3");

      // remove
      f = c.removeAsync("k");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v3");
      assert c.get("k") == null;

      // putIfAbsent again
      f = c.putIfAbsentAsync("k", "v4");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k").equals("v4");

      // removecond
      Future<Boolean> f3 = c.removeAsync("k", "v_nonexistent");
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert c.get("k").equals("v4");

      f3 = c.removeAsync("k", "v4");
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert c.get("k") == null;

      // replace
      f = c.replaceAsync("k", "v5");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k") == null;

      c.put("k", "v");
      f = c.replaceAsync("k", "v5");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert c.get("k").equals("v5");

      //replace2
      f3 = c.replaceAsync("k", "v_nonexistent", "v6");
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert c.get("k").equals("v5");

      f3 = c.replaceAsync("k", "v5", "v6");
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert c.get("k").equals("v6");
   }
}
