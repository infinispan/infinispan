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
package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Key;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Test(groups = "functional", testName = "replication.AsyncAPISyncReplTest")
public class AsyncAPISyncReplTest extends MultipleCacheManagersTest {

   protected Cache<Key, String> c1, c2;

   @SuppressWarnings("unchecked")
   protected void createCacheManagers() throws Throwable {
      Configuration c =
            getDefaultClusteredConfig(sync() ? Configuration.CacheMode.REPL_SYNC : Configuration.CacheMode.REPL_ASYNC, true);
      List<Cache<Key, String>> l = createClusteredCaches(2, getClass().getSimpleName(), c);
      c1 = l.get(0);
      c2 = l.get(1);
   }

   protected boolean sync() {
      return true;
   }

   protected void asyncWait(boolean tx, Class<? extends WriteCommand>... cms) {
   }

   protected void resetListeners() {
   }  

   protected void assertOnAllCaches(Key k, String v) {
      Object real;
      assert Util.safeEquals((real = c1.get(k)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
      assert Util.safeEquals((real = c2.get(k)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
   }

   public void testAsyncMethods() throws ExecutionException, InterruptedException {

      String v = "v";
      String v2 = "v2";
      String v3 = "v3";
      String v4 = "v4";
      String v5 = "v5";
      String v6 = "v6";
      String v_null = "v_nonexistent";
      Key key = new Key("k", true);

      // put
      Future<String> f = c1.putAsync(key, v);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key) == null;
      log.info("*** Allowing serialization on key");
      key.allowSerialization();
      log.info("*** Finished allowing serialization on key, checking future if cancelled");
      assert !f.isCancelled();
      log.info("*** Future not cancelled, checking future.get()");
      assert f.get() == null;
      assert f.isDone();
      assertOnAllCaches(key, v);

      f = c1.putAsync(key, v2);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get().equals(v);
      assert f.isDone();
      assertOnAllCaches(key, v2);

      // putAll
      Future<Void> f2 = c1.putAllAsync(Collections.singletonMap(key, v3));
      assert f2 != null;
      assert !f2.isDone();
      assert c2.get(key).equals(v2);
      key.allowSerialization();
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      assertOnAllCaches(key, v3);

      // putIfAbsent
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert c2.get(key).equals(v3);
      assert !f.isCancelled();
      assert f.get().equals(v3);
      assert f.isDone();
      assertOnAllCaches(key, v3);

      // remove
      f = c1.removeAsync(key);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v3);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get().equals(v3);
      assert f.isDone();
      assertOnAllCaches(key, null);

      // putIfAbsent again
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key) == null;
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assertOnAllCaches(key, v4);

      // removecond
      Future<Boolean> f3 = c1.removeAsync(key, v_null);
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      assertOnAllCaches(key, v4);

      f3 = c1.removeAsync(key, v4);
      assert f3 != null;
      assert !f3.isDone();
      assert c2.get(key).equals(v4);
      key.allowSerialization();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      assertOnAllCaches(key, null);

      // replace
      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assertOnAllCaches(key, null);

      key.allowSerialization();
      resetListeners();
      c1.put(key, v);
      asyncWait(false, PutKeyValueCommand.class);

      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert !f.isDone();
      assert c2.get(key).equals(v);
      key.allowSerialization();
      assert !f.isCancelled();
      assert f.get().equals(v);
      assert f.isDone();
      assertOnAllCaches(key, v5);

      //replace2
      f3 = c1.replaceAsync(key, v_null, v6);
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      assertOnAllCaches(key, v5);

      f3 = c1.replaceAsync(key, v5, v6);
      assert f3 != null;
      assert !f3.isDone();
      assert c2.get(key).equals(v5);
      key.allowSerialization();
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      assertOnAllCaches(key, v6);
   }

   public void testAsyncTxMethods() throws Exception {

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
      assert f.isDone();
      Transaction t = tm.suspend();
      assert c2.get(key) == null;
      tm.resume(t);
      assert f.get() == null;
      tm.commit();
      asyncWait(true, PutKeyValueCommand.class);
      assertOnAllCaches(key, v);

      tm.begin();
      f = c1.putAsync(key, v2);
      assert f != null;
      assert f.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v);
      tm.resume(t);
      assert !f.isCancelled();
      assert f.get().equals(v);
      tm.commit();
      asyncWait(true, PutKeyValueCommand.class);
      assertOnAllCaches(key, v2);

      // putAll
      tm.begin();
      Future<Void> f2 = c1.putAllAsync(Collections.singletonMap(key, v3));
      assert f2 != null;
      assert f2.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v2);
      tm.resume(t);
      assert !f2.isCancelled();
      assert f2.get() == null;
      tm.commit();
      asyncWait(true, PutMapCommand.class);
      assertOnAllCaches(key, v3);

      // putIfAbsent
      tm.begin();
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert f.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v3);
      tm.resume(t);
      assert !f.isCancelled();
      assert f.get().equals(v3);
      tm.commit();
      assertOnAllCaches(key, v3);

      // remove
      tm.begin();
      f = c1.removeAsync(key);
      assert f != null;
      assert f.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v3);
      tm.resume(t);
      assert !f.isCancelled();
      assert f.get().equals(v3);
      tm.commit();
      asyncWait(true, RemoveCommand.class);
      assertOnAllCaches(key, null);

      // putIfAbsent again
      tm.begin();
      f = c1.putIfAbsentAsync(key, v4);
      assert f != null;
      assert f.isDone();
      t = tm.suspend();
      assert c2.get(key) == null;
      tm.resume(t);
      assert !f.isCancelled();
      assert f.get() == null;
      tm.commit();
      asyncWait(true, PutKeyValueCommand.class);
      assertOnAllCaches(key, v4);

      // removecond
      tm.begin();
      Future<Boolean> f3 = c1.removeAsync(key, v_null);
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      tm.commit();
      assertOnAllCaches(key, v4);

      tm.begin();
      f3 = c1.removeAsync(key, v4);
      assert f3 != null;
      assert f3.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v4);
      tm.resume(t);
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      tm.commit();
      asyncWait(true, RemoveCommand.class);
      assertOnAllCaches(key, null);

      // replace
      tm.begin();
      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      tm.commit();
      assertOnAllCaches(key, null);

      c1.put(key, v);
      asyncWait(false, PutKeyValueCommand.class);

      tm.begin();
      f = c1.replaceAsync(key, v5);
      assert f != null;
      assert f.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v);
      tm.resume(t);
      assert !f.isCancelled();
      assert f.get().equals(v);
      tm.commit();
      asyncWait(true, ReplaceCommand.class);
      assertOnAllCaches(key, v5);

      //replace2
      tm.begin();
      f3 = c1.replaceAsync(key, v_null, v6);
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      tm.commit();
      assertOnAllCaches(key, v5);

      tm.begin();
      f3 = c1.replaceAsync(key, v5, v6);
      assert f3 != null;
      assert f3.isDone();
      t = tm.suspend();
      assert c2.get(key).equals(v5);
      tm.resume(t);
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      tm.commit();
      asyncWait(true, ReplaceCommand.class);
      assertOnAllCaches(key, v6);
   }
}
