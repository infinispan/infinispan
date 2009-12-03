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
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * FunctionalTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "server.memcached.FunctionalTest")
public class FunctionalTest extends SingleCacheManagerTest {
   private MemcachedClient client;
   private MemcachedTextServer server;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      server = new MemcachedTextServer(cacheManager);
      server.start();
      DefaultConnectionFactory d = new DefaultConnectionFactory() {
         @Override
         public long getOperationTimeout() {
            return 5000;
         }
      };
      
      client = new MemcachedClient(d, Arrays.asList(new InetSocketAddress[]{new InetSocketAddress(11211)}));
      return cacheManager;
   }

   public void testBasicSet(Method m) throws Exception {
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

   public void testBasicAdd(Method m) throws Exception {
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

   public void testBasicReplace(Method m) throws Exception {
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

   public void testBasicAppend(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));

      f = client.append(0, k(m), v(m, "v1-"));
      assert f.get(5, TimeUnit.SECONDS);
      String expected = v(m).toString() + v(m, "v1-").toString();
      assert expected.equals(client.get(k(m)));
   }

   public void testBasicPrepend(Method m) throws Exception {
      Future<Boolean> f = client.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));

      f = client.prepend(0, k(m), v(m, "v1-"));
      assert f.get(5, TimeUnit.SECONDS);
      String expected = v(m, "v1-").toString() + v(m).toString();
      assert expected.equals(client.get(k(m)));
   }

   public void testBasicGets(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;
   }

   public void testBasicCas(Method m) throws Exception {
      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;

      CASResponse resp = client.cas(k(m), value.getCas(), v(m, "k1-"));
      assert CASResponse.OK == resp;
   }

   private String k(Method method, String prefix) {
      return prefix + method.getName();
   }

   private Object v(Method method, String prefix) {
      return prefix  + method.getName();
   }

   private String k(Method method) {
      return k(method, "k-");
   }

   private Object v(Method method) {
      return v(method, "v-");
   }

}
