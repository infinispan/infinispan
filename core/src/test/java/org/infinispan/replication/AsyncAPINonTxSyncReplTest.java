/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.replication;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.data.Key;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Test(groups="functional", testName = "replication.AsyncAPINonTxSyncReplTest")
public class AsyncAPINonTxSyncReplTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfig();
      c.transaction().autoCommit(false);
      createClusteredCaches(2, c);
   }

   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(sync() ? CacheMode.REPL_SYNC : CacheMode.REPL_ASYNC, false);
   }

   protected boolean sync() {
      return true;
   }

   public void testAsyncMethods() throws Exception {
      final Cache c1 = cache(0);
      final Cache c2 = cache(1);


      final String v = "v";
      final String v2 = "v2";
      final String v3 = "v3";
      final String v4 = "v4";
      final String v5 = "v5";
      final String v6 = "v6";
      final String v_null = "v_nonexistent";
      final Key key = new Key("k", true);

      log.trace("Before put");
      Future<String> f = c1.putAsync(key, v);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key) == null;
      log.info("*** Allowing serialization on key");
      key.allowSerialization();
      log.info("*** Finished allowing serialization on key, checking future if cancelled");
      assert !f.isCancelled();
      log.info("*** Future not cancelled, checking future.get()");
      assertFutureValue(f, null);
      assert f.isDone();
      assertOnAllCaches(key, v, c1, c2);

      log.trace("Before put2");
      f = c1.putAsync(key, v2);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assertFutureValue(f, v);
      assert f.isDone();
      assertOnAllCaches(key, v2, c1, c2);

      log.trace("Before putAll");
      Future<Void> f2 = c1.putAllAsync(Collections.singletonMap(key, v3));
      assert f2 != null;
      assert !f2.isDone();
      assert c2.get(key).equals(v2);
      key.allowSerialization();
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      assertOnAllCaches(key, v3, c1, c2);

      log.trace("Before putIfAbsent");
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert c2.get(key).equals(v3);
      if (!isLockOwner(c1, key))
         key.allowSerialization();
      assert !f.isCancelled();
      assertFutureValue(f, v3);
      assert f.isDone();
      assertOnAllCaches(key, v3, c1, c2);

      log.trace("Before remove");
      f = c1.removeAsync(key);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v3);
      key.allowSerialization();
      assert !f.isCancelled();
      assertFutureValue(f, v3);
      assert f.isDone();
      assertOnAllCaches(key, null, c1, c2);

      log.trace("Before putIfAbsentAsync");
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert !f.isDone();
      key.allowSerialization();
      assert !f.isCancelled();
      assertFutureValue(f, null);
      assert f.isDone();
      assertOnAllCaches(key, v4, c1, c2);

      log.trace("Before conditional removeAsync");
      Future<Boolean> f3 = c1.removeAsync(key, v_null);
      assert f3 != null;
      assert !f3.isCancelled();
      if (!isLockOwner(c1, key))
         key.allowSerialization();
      assertFutureValue(f3, Boolean.FALSE);
      assert f3.isDone();
      assertOnAllCaches(key, v4, c1, c2);

      log.trace("Before conditional removeAsync2");
      f3 = c1.removeAsync(key, v4);
      assert f3 != null;
      assert !f3.isDone();
      assert c2.get(key).equals(v4);
      key.allowSerialization();
      assert !f3.isCancelled();
      assertFutureValue(f3, true);
      assert f3.isDone();
      assertOnAllCaches(key, null, c1, c2);

      log.trace("Before replaceAsync");
      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert !f.isCancelled();
      if (!isLockOwner(c1, key))
         key.allowSerialization();
      assertFutureValue(f, null);
      assert f.isDone();
      assertOnAllCaches(key, null, c1, c2);

      log.trace("Before put(k,v) " + key + ", " + v);
      key.allowSerialization();
      resetListeners();
      c1.put(key, v);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return v.equals(c1.get(key)) && v.equals(c2.get(key));
         }
      });

      log.trace("After put(k,v) " + key + ", " + v);

      log.trace("Before replaceAsync2");
      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assertFutureValue(f, v);
      assert f.isDone();
      assertOnAllCaches(key, v5, c1, c2);

      log.trace("Before replaceAsync3");
      f3 = c1.replaceAsync(key, v_null, v6);
      assert f3 != null;
      assert !f3.isCancelled();
      if (!isLockOwner(c1, key))
         key.allowSerialization();
      assertFutureValue(f3, false);
      assert f3.isDone();
      assertOnAllCaches(key, v5, c1, c2);

      log.trace("Before replaceAsync4");
      f3 = c1.replaceAsync(key, v5, v6);
      assert f3 != null;
      assert !f3.isDone();
      assert c2.get(key).equals(v5);
      key.allowSerialization();
      assert !f3.isCancelled();
      assertFutureValue(f3, true);
      assert f3.isDone();
      assertOnAllCaches(key, v6, c1, c2);
   }

   protected void assertFutureValue(Future f, Object value) throws ExecutionException, InterruptedException {
      assert Util.safeEquals(f.get(), value);
   }

   protected void assertOnAllCaches(final Key k, final String v, final Cache c1, final Cache c2) {
      if (sync()) {
         Object real;
         assert Util.safeEquals((real = c1.get(k)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
         assert Util.safeEquals((real = c2.get(k)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
      } else {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return Util.safeEquals(c1.get(k), v) && Util.safeEquals(c2.get(k), v);
            }
         });
      }
   }

   protected void resetListeners() {
   }

   private boolean isLockOwner(Cache cache, Object key) {
      AdvancedCache advancedCache = cache.getAdvancedCache();
      Address primaryLocation = advancedCache.getDistributionManager().getPrimaryLocation(key);
      Address localAddress = advancedCache.getRpcManager().getAddress();
      boolean isOwner = primaryLocation.equals(localAddress);
      log.tracef("Is %s lock owner? %s", localAddress, isOwner);
      return isOwner;
   }
}
