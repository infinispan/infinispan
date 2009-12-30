/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.memcached;

import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;

import org.infinispan.Version;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.memcached.test.MemcachedTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.*;

/**
 * FunctionalTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "server.memcached.FunctionalTest")
public class FunctionalTest extends SingleCacheManagerTest {
   MemcachedClient client;
   MemcachedTextServer server;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      server = MemcachedTestingUtil.createMemcachedTextServer(cacheManager);
      server.start();
      client = createMemcachedClient(60000, server.getPort());
      return cacheManager;
   }

   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      server.stop();
   }
   
   public void testSetBasic(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));
   }

   public void testSetWithExpirySeconds(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 1, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      Thread.sleep(1100);
      assert null == client.get(k(m));
   }

   public void testSetWithExpiryUnixTime(Method m) throws Exception {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      Future<Boolean> f = client.set(k(m), future, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      Thread.sleep(1100);
      assert null == client.get(k(m));
   }

   public void testGetMultipleKeys(Method m) throws Exception {
      Future<Boolean> f1 = client.set(k(m, "k1-"), 0, v(m, "v1-"));
      Future<Boolean> f2 = client.set(k(m, "k2-"), 0, v(m, "v2-"));
      Future<Boolean> f3 = client.set(k(m, "k3-"), 0, v(m, "v3-"));
      assert f1.get(5, TimeUnit.SECONDS);
      assert f2.get(5, TimeUnit.SECONDS);
      assert f3.get(5, TimeUnit.SECONDS);
      
      Map<String, Object> ret = client.getBulk(Arrays.asList(new String[]{k(m, "k1-"), k(m, "k2-"), k(m, "k3-")}));
      assert ret.get(k(m, "k1-")).equals(v(m, "v1-"));
      assert ret.get(k(m, "k2-")).equals(v(m, "v2-"));
      assert ret.get(k(m, "k3-")).equals(v(m, "v3-"));
   }

   public void testAddBasic(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));
   }

   public void testAddWithExpirySeconds(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 1, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      Thread.sleep(1100);
      assert null == client.get(k(m));

      f = client.add(k(m), 0, v(m, "k1-"));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m, "k1-").equals(client.get(k(m)));
   }

   public void testAddWithExpiryUnixTime(Method m) throws Exception {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      Future<Boolean> f = client.add(k(m), future, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      Thread.sleep(1100);
      assert null == client.get(k(m));

      f = client.add(k(m), 0, v(m, "k1-"));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m, "k1-").equals(client.get(k(m)));
   }

   public void testNotAddIsPresent(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert(f.get(5, TimeUnit.SECONDS));
      assert v(m).equals(client.get(k(m)));

      f = client.add(k(m), 0, v(m, "k1-"));
      assert false == f.get(5, TimeUnit.SECONDS);
      assert client.get(k(m)).equals(v(m));
   }

   public void testReplaceBasic(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert(f.get(5, TimeUnit.SECONDS));
      assert v(m).equals(client.get(k(m)));
      
      f = client.replace(k(m), 0, v(m, "k1-"));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m, "k1-").equals(client.get(k(m)));
   }

   public void testNotReplaceIsNotPresent(Method m) throws Exception {
      Future<Boolean> f = client.replace(k(m), 0, v(m));
      assert false == f.get(5, TimeUnit.SECONDS);
      assert null == client.get(k(m));
   }

   public void testReplaceWithExpirySeconds(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert(f.get(5, TimeUnit.SECONDS));
      assert v(m).equals(client.get(k(m)));
      
      f = client.replace(k(m), 1, v(m, "k1-"));
      assert f.get(5, TimeUnit.SECONDS);
      assert client.get(k(m)).equals(v(m, "k1-"));
      Thread.sleep(1100);
      assert null == client.get(k(m));
   }

   public void testReplaceWithExpiryUnixTime(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));

      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      f = client.replace(k(m), future, v(m, "k1-"));
      assert f.get(5, TimeUnit.SECONDS);
      assert client.get(k(m)).equals(v(m, "k1-"));
      Thread.sleep(1100);
      assert null == client.get(k(m));
   }

   public void testAppendBasic(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));

      f = client.append(0, k(m), v(m, "v1-"));
      assert f.get(5, TimeUnit.SECONDS);
      String expected = v(m).toString() + v(m, "v1-").toString();
      assert expected.equals(client.get(k(m)));
   }

   public void testPrependBasic(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));

      f = client.prepend(0, k(m), v(m, "v1-"));
      assert f.get(5, TimeUnit.SECONDS);
      String expected = v(m, "v1-").toString() + v(m).toString();
      assert expected.equals(client.get(k(m)));
   }

   public void testGetsBasic(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;
   }

   public void testCasBasic(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(60, TimeUnit.SECONDS);
      CASValue<Object> value = client.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;

      CASResponse resp = client.cas(k(m), value.getCas(), v(m, "v1-"));
      assert CASResponse.OK == resp;
   }

   public void testCasNotFound(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;

      CASResponse resp = client.cas(k(m, "k1-"), value.getCas(), v(m, "v1-"));
      assert CASResponse.NOT_FOUND == resp;
   }

   public void testCasExists(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;

      long old = value.getCas();
      CASResponse resp = client.cas(k(m), value.getCas(), v(m, "v1-"));
      value = client.gets(k(m));
      assert v(m, "v1-").equals(value.getValue());
      assert value.getCas() != 0;
      assert value.getCas() != old;

      resp = client.cas(k(m), old, v(m, "v2-"));
      assert CASResponse.EXISTS == resp;

      resp = client.cas(k(m), value.getCas(), v(m, "v2-"));
      assert CASResponse.OK == resp;
   }

   public void testDeleteBasic(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      f = client.delete(k(m));
      assert f.get(5, TimeUnit.SECONDS);
   }

   public void testDeleteDoesNotExist(Method m) throws Exception {
      Future<Boolean> f = client.delete(k(m));
      assert !f.get(5, TimeUnit.SECONDS);
   }

   public void testIncrementBasic(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, 1);
      assert f.get(5, TimeUnit.SECONDS);
      assert 2 == client.incr(k(m), 1);
   }

   public void testIncrementTriple(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, 1);
      assert f.get(5, TimeUnit.SECONDS);
      assert 2 == client.incr(k(m), 1);
      assert 4 == client.incr(k(m), 2);
      assert 8 == client.incr(k(m), 4);
   }

   public void testIncrementNotExist(Method m) throws Exception {
      assert -1 == client.incr(k(m), 1);
   }

   public void testIncrementIntegerMax(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, 0);
      assert f.get(5, TimeUnit.SECONDS);
      assert Integer.MAX_VALUE == client.incr(k(m), Integer.MAX_VALUE);
   }

   public void testIncrementBeyondIntegerMax(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, 1);
      assert f.get(5, TimeUnit.SECONDS);
      long newValue = client.incr(k(m), Integer.MAX_VALUE);
      assert new Long(Integer.MAX_VALUE) + 1 == newValue : "New value not expected: " + newValue;
   }

   public void testDecrementBasic(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, 1);
      assert f.get(5, TimeUnit.SECONDS);
      assert 0 == client.decr(k(m), 1);
   }

   public void testDecrementTriple(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, 8);
      assert f.get(5, TimeUnit.SECONDS);
      assert 7 == client.decr(k(m), 1);
      assert 5 == client.decr(k(m), 2);
      assert 1 == client.decr(k(m), 4);
   }

   public void testDecrementNotExist(Method m) throws Exception {
      assert -1 == client.decr(k(m), 1);
   }

   public void testDecrementBelowZero(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, 1);
      assert f.get(5, TimeUnit.SECONDS);
      long newValue = client.decr(k(m), 2);
      assert 0 ==  newValue : "Unexpected result: " + newValue;
   }

   public void testFlushAll(Method m) throws Exception {
      Future<Boolean> f;
      for (int i = 0; i < 5; i++) {
         String key = k(m, "k" + i + "-");
         Object value = v(m, "v" + i + "-");
         f = client.set(key, 0, value);
         assert f.get(5, TimeUnit.SECONDS);
         assert value.equals(client.get(key));
      }

      f = client.flush();
      assert f.get(5, TimeUnit.SECONDS);

      for (int i = 0; i < 5; i++) {
         String key = k(m, "k" + i + "-");
         assert null == client.get(key);
      }
   }

   public void testFlushAllDelayed(Method m) throws Exception {
      Future<Boolean> f;
      for (int i = 0; i < 5; i++) {
         String key = k(m, "k" + i + "-");
         Object value = v(m, "v" + i + "-");
         f = client.set(key, 0, value);
         assert f.get(5, TimeUnit.SECONDS);
         assert value.equals(client.get(key));
      }

      f = client.flush(2);
      assert f.get(5, TimeUnit.SECONDS);

      TestingUtil.sleepThread(2200);

      for (int i = 0; i < 5; i++) {
         String key = k(m, "k" + i + "-");
         assert null == client.get(key);
      }
   }

   public void testVersion() throws Exception {
      Map<SocketAddress, String> versions = client.getVersions();
      assert 1 == versions.size();
      String version = versions.values().iterator().next();
      assert Version.version.equals(version);
   }
}
