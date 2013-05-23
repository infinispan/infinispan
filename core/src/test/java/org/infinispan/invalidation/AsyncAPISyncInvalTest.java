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
package org.infinispan.invalidation;

import org.infinispan.Cache;
import org.infinispan.commands.write.WriteCommand;
import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.data.Key;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Test(groups = "functional", testName = "invalidation.AsyncAPISyncInvalTest")
public class AsyncAPISyncInvalTest extends MultipleCacheManagersTest {   

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(sync() ? CacheMode.INVALIDATION_SYNC : CacheMode.INVALIDATION_ASYNC, false);
      createClusteredCaches(2, getClass().getSimpleName(), c);
   }

   protected boolean sync() {
      return true;
   }

   protected void asyncWait(Class<? extends WriteCommand>... cmds) {
   }

   protected void resetListeners() {
   }

   protected void assertInvalidated(final Key k, final String value) {
      final String cacheName = getClass().getSimpleName();
      if (sync()) {
         Cache c1 = cache(0, cacheName);
         Cache c2 = cache(1, cacheName);
         assert Util.safeEquals(c1.get(k), value);
         assert !c2.containsKey(k);
      } else {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               Object v0 = cache(0, cacheName).get(k);
               return Util.safeEquals(v0, value) && !cache(1, cacheName).containsKey(k);
            }
         });
      }
   }

   private void initC2(Key k) {
      Cache c1 = cache(0,getClass().getSimpleName());
      Cache c2 = cache(1,getClass().getSimpleName());
      c2.getAdvancedCache().withFlags(CACHE_MODE_LOCAL).put(k, "v");
   }

   public void testAsyncMethods() throws ExecutionException, InterruptedException {

      Cache c1 = cache(0,getClass().getSimpleName());
      Cache c2 = cache(1,getClass().getSimpleName());
      String v = "v";
      String v2 = "v2";
      String v3 = "v3";
      String v4 = "v4";
      String v5 = "v5";
      String v6 = "v6";
      String v_null = "v_nonexistent";
      Key key = new Key("k", true);

      initC2(key);

      assert !c1.containsKey(key);
      assert v.equals(c2.get(key));

      log.trace("Here it is");
      // put
      Future<String> f = c1.putAsync(key, v);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assertInvalidated(key, v);

      initC2(key);
      f = c1.putAsync(key, v2);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get().equals(v);
      assert f.isDone();
      assertInvalidated(key, v2);

      // putAll
      initC2(key);
      Future<Void> f2 = c1.putAllAsync(Collections.singletonMap(key, v3));
      assert f2 != null;
      assert !f2.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      assertInvalidated(key, v3);

      // putIfAbsent
      initC2(key);
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert c2.get(key).equals(v);
      assert !f.isCancelled();
      assert f.get().equals(v3);
      assert f.isDone();

      // remove
      initC2(key);
      f = c1.removeAsync(key);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get().equals(v3);
      assert f.isDone();
      assertInvalidated(key, null);

      // putIfAbsent again
      initC2(key);
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assertInvalidated(key, v4);

      // removecond
      initC2(key);
      Future<Boolean> f3 = c1.removeAsync(key, v_null);
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      assert c2.get(key).equals(v);

      f3 = c1.removeAsync(key, v4);
      assert f3 != null;
      assert !f3.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      assertInvalidated(key, null);

      // replace
      initC2(key);
      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assert c2.get(key).equals(v);

      key.allowSerialization();
      resetListeners();
      c1.put(key, v);
      asyncWait();

      initC2(key);
      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get().equals(v);
      assert f.isDone();
      assertInvalidated(key, v5);

      //replace2
      initC2(key);
      f3 = c1.replaceAsync(key, v_null, v6);
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      assert c2.get(key).equals(v);
      assert c1.get(key).equals(v5);

      f3 = c1.replaceAsync(key, v5, v6);
      assert f3 != null;
      assert !f3.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      assertInvalidated(key, v6);
   }
}