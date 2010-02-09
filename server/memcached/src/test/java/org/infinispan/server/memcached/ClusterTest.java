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

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.k;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.v;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;

import org.infinispan.config.Configuration;
import org.infinispan.server.memcached.test.MemcachedTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * ClusterTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "server.memcached.ClusterTest")
public class ClusterTest extends MultipleCacheManagersTest {
   MemcachedClient client1;
   MemcachedClient client2;
   TextServer server1;
   TextServer server2;
   
   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      createClusteredCaches(2, "replSync", replSync);
      server1 = MemcachedTestingUtil.createMemcachedTextServer(cache(0, "replSync"));
      server1.start();
      server2 = MemcachedTestingUtil.createMemcachedTextServer(cache(1, "replSync"), server1.getPort() + 50);
      server2.start();
      client1 = createMemcachedClient(60000, server1.getPort());
      client2 = createMemcachedClient(60000, server2.getPort());
   }

   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      server1.stop();
      server2.stop();
   }

   public void testReplicatedSet(Method m) throws Exception {
      Future<Boolean> f = client1.set(k(m), 0, v(m));
      assert f.get(120, TimeUnit.SECONDS);
      assert v(m).equals(client2.get(k(m)));
   }

   public void testReplicatedGetMultipleKeys(Method m) throws Exception {
      Future<Boolean> f1 = client1.set(k(m, "k1-"), 0, v(m, "v1-"));
      Future<Boolean> f2 = client1.set(k(m, "k2-"), 0, v(m, "v2-"));
      Future<Boolean> f3 = client1.set(k(m, "k3-"), 0, v(m, "v3-"));
      assert f1.get(5, TimeUnit.SECONDS);
      assert f2.get(5, TimeUnit.SECONDS);
      assert f3.get(5, TimeUnit.SECONDS);
      
      Map<String, Object> ret = client2.getBulk(Arrays.asList(new String[]{k(m, "k1-"), k(m, "k2-"), k(m, "k3-")}));
      assert ret.get(k(m, "k1-")).equals(v(m, "v1-"));
      assert ret.get(k(m, "k2-")).equals(v(m, "v2-"));
      assert ret.get(k(m, "k3-")).equals(v(m, "v3-"));
   }

   public void testReplicatedAdd(Method m) throws Exception {
      Future<Boolean> f = client1.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client2.get(k(m)));
   }

   public void testReplicatedReplace(Method m) throws Exception {
      Future<Boolean> f = client1.add(k(m), 0, v(m));
      assert(f.get(5, TimeUnit.SECONDS));
      assert v(m).equals(client2.get(k(m)));
      
      f = client2.replace(k(m), 0, v(m, "k1-"));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m, "k1-").equals(client1.get(k(m)));
   }

   public void testReplicatedAppend(Method m) throws Exception {
      Future<Boolean> f = client1.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client2.get(k(m)));

      f = client2.append(0, k(m), v(m, "v1-"));
      assert f.get(5, TimeUnit.SECONDS);
      String expected = v(m).toString() + v(m, "v1-").toString();
      assert expected.equals(client1.get(k(m)));
   }

   public void testReplicatedPrepend(Method m) throws Exception {
      Future<Boolean> f = client1.add(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client2.get(k(m)));

      f = client2.prepend(0, k(m), v(m, "v1-"));
      assert f.get(5, TimeUnit.SECONDS);
      String expected = v(m, "v1-").toString() + v(m).toString();
      assert expected.equals(client1.get(k(m)));
   }

   public void testReplicatedGets(Method m) throws Exception {
      Future<Boolean> f = client1.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client2.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;
   }

   public void testReplicatedCasExists(Method m) throws Exception {
      Future<Boolean> f = client1.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client2.gets(k(m));
      assert v(m).equals(value.getValue());
      assert value.getCas() != 0;

      long old = value.getCas();
      CASResponse resp = client2.cas(k(m), value.getCas(), v(m, "v1-"));
      value = client1.gets(k(m));
      assert v(m, "v1-").equals(value.getValue());
      assert value.getCas() != 0;
      assert value.getCas() != old;

      resp = client1.cas(k(m), old, v(m, "v2-"));
      assert CASResponse.EXISTS == resp;

      resp = client2.cas(k(m), value.getCas(), v(m, "v2-"));
      assert CASResponse.OK == resp;
   }

   public void testReplicatedDelete(Method m) throws Exception {
      Future<Boolean> f = client1.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      f = client2.delete(k(m));
      assert f.get(5, TimeUnit.SECONDS);
   }

   public void testReplicatedIncrement(Method m) throws Exception {
      Future<Boolean> f = client1.set(k(m), 0, "1");
      assert f.get(5, TimeUnit.SECONDS);
      assert 2 == client2.incr(k(m), 1);
   }

   public void testReplicatedDecrement(Method m) throws Exception {
      Future<Boolean> f = client1.set(k(m), 0, "1");
      assert f.get(5, TimeUnit.SECONDS);
      assert 0 == client2.decr(k(m), 1);
   }

}
