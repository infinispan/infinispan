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
package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "api.AsyncAPITest")
public class AsyncAPITest extends SingleCacheManagerTest {
   Cache<String, String> c;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      c = cm.getCache();
      return cm;
   }
  
   public void testAsyncMethods() throws ExecutionException, InterruptedException {
      // get
      Future<String> f = c.getAsync("k");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k") == null;

      // put
      f = c.putAsync("k", "v");
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

      // get
      f = c.getAsync("k");
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v4");
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

   public void testAsyncMethodWithLifespanAndMaxIdle() throws ExecutionException, InterruptedException {

      // lifespan only
      Future<String> f = c.putAsync("k", "v", 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      verifyLifespanOnly("k", "v");

      // lifespan and max idle (test max idle)
      f = c.putAsync("k", "v", 500, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      verifyMaxIdle("k", "v");

      // lifespan and max idle (test lifespan)
      f = c.putAsync("k", "v", 300, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      verifyLifespan("k", "v");

      // putAll lifespan only
      Future<Void> f2 = c.putAllAsync(Collections.singletonMap("k", "v3"), 200, TimeUnit.MILLISECONDS);
      assert f2 != null;
      assert f2.isDone();
      assert !f2.isCancelled();
      assert f2.get() == null;
      verifyLifespanOnly("k", "v3");

      // putAll lifespan and max idle (test max idle)
      f2 = c.putAllAsync(Collections.singletonMap("k", "v4"), 500, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f2 != null;
      assert f2.isDone();
      assert !f2.isCancelled();
      assert f2.get() == null;
      verifyMaxIdle("k", "v4");

      // putAll lifespan and max idle (test lifespan)
      f2 = c.putAllAsync(Collections.singletonMap("k", "v5"), 300, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f2 != null;
      assert f2.isDone();
      assert !f2.isCancelled();
      assert f2.get() == null;
      verifyLifespan("k", "v5");

      // putIfAbsent lifespan only
      c.putAsync("k", "v3");
      f = c.putIfAbsentAsync("k", "v4", 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v3");
      assert c.get("k").equals("v3");
      assert !c.get("k").equals("v4");
      Thread.sleep(300);
      assert c.get("k").equals("v3");
      c.removeAsync("k");
      assert c.get("k") == null;

      // now really put (k removed) lifespan only
      f = c.putIfAbsentAsync("k", "v", 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      verifyLifespanOnly("k", "v");

      // putIfAbsent lifespan and max idle (test max idle)
      f = c.putIfAbsentAsync("k", "v", 500, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      verifyMaxIdle("k", "v");

      // putIfAbsent lifespan and max idle (test lifespan)
      f = c.putIfAbsentAsync("k", "v", 300, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      verifyLifespan("k", "v");

      // replace
      f = c.replaceAsync("k", "v5", 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get() == null;
      assert c.get("k") == null;

      // replace lifespan only
      c.put("k", "v");
      f = c.replaceAsync("k", "v5", 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v");
      verifyLifespanOnly("k", "v5");

      // replace lifespan and max idle (test max idle)
      c.put("k", "v");
      f = c.replaceAsync("k", "v5", 500, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v");
      verifyMaxIdle("k", "v5");

      // replace lifespan and max idle (test lifespan)
      c.put("k", "v");
      f = c.replaceAsync("k", "v5", 300, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f != null;
      assert f.isDone();
      assert !f.isCancelled();
      assert f.get().equals("v");
      verifyLifespan("k", "v5");

      //replace2
      c.put("k", "v5");
      Future<Boolean> f3 = c.replaceAsync("k", "v_nonexistent", "v6", 200, TimeUnit.MILLISECONDS);
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      Thread.sleep(300);
      assert c.get("k").equals("v5");

      // replace2 lifespan only
      f3 = c.replaceAsync("k", "v5", "v6", 200, TimeUnit.MILLISECONDS);
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      verifyLifespanOnly("k", "v6");

      // replace2 lifespan and max idle (test max idle)
      c.put("k", "v5");
      f3 = c.replaceAsync("k", "v5", "v6", 500, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      verifyMaxIdle("k", "v6");

      // replace2 lifespan and max idle (test lifespan)
      c.put("k", "v5");
      f3 = c.replaceAsync("k", "v5", "v6", 300, TimeUnit.MILLISECONDS, 200, TimeUnit.MILLISECONDS);
      assert f3 != null;
      assert f3.isDone();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      verifyLifespan("k", "v6");
   }

   // for verification operations using lifespan ONLY
   private void verifyLifespanOnly(String k, String expectedValue) throws InterruptedException {
      assert c.get(k).equals(expectedValue);
      assert c.get(k).equals(expectedValue);
      Thread.sleep(300);
      assert c.get("k") == null;
   }

   // for verification operations using lifespan AND max idle parameters
   private void verifyLifespan(String k, String expectedValue) throws InterruptedException {
      assert c.get(k).equals(expectedValue);
      Thread.sleep(100);
      assert c.get(k).equals(expectedValue);
      Thread.sleep(100);
      assert c.get(k).equals(expectedValue);
      Thread.sleep(150);
      assert c.get(k) == null;
   }

   // for verification operations using lifespan AND max idle parameters
   private void verifyMaxIdle(String k, String expectedValue) throws InterruptedException {
      assert c.get(k).equals(expectedValue);
      Thread.sleep(100);
      assert c.get(k).equals(expectedValue);
      Thread.sleep(300);
      assert c.get(k) == null;
   }
}
