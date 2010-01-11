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
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
 * StatsTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "server.memcached.StatsTest")
public class StatsTest extends SingleCacheManagerTest {
   static final String JMX_DOMAIN = StatsTest.class.getSimpleName();
   MemcachedClient client;
   TextServer server;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createJmxEnabledCacheManager(JMX_DOMAIN);
      server = MemcachedTestingUtil.createMemcachedTextServer(cacheManager.getCache());
      server.start();
      client = createMemcachedClient(60000, server.getPort());
      return cacheManager;
   }

   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      server.stop();
   }

   public void testUnsupportedStats(Method m) throws Exception {
      Map<String, String> stats = getStats();
      assert "0".equals(stats.get("pid"));
      assert "0".equals(stats.get("pointer_size"));
      assert "0".equals(stats.get("rusage_user"));
      assert "0".equals(stats.get("rusage_system"));
      assert "0".equals(stats.get("bytes"));
      assert "0".equals(stats.get("connection_structures"));
      assert "0".equals(stats.get("auth_cmds"));
      assert "0".equals(stats.get("auth_errors"));
      assert "0".equals(stats.get("limit_maxbytes"));
      assert "0".equals(stats.get("conn_yields"));
   }

   public void testUncomparableStats(Method m) throws Exception {
      TestingUtil.sleepThread(TimeUnit.SECONDS.toMillis(1));
      Map<String, String> stats = getStats();
      assert !"0".equals(stats.get("uptime"));
      assert !"0".equals(stats.get("time"));
      assert !stats.get("uptime").equals(stats.get("time"));
   }

   public void testStaticStats(Method m) throws Exception {
      Map<String, String> stats = getStats();
      assert Version.version.equals(stats.get("version"));
   }

   public void testTodoStats() throws Exception {
      Map<String, String> stats = getStats();
      assert "0".equals(stats.get("curr_connections"));
      assert "0".equals(stats.get("total_connections"));
      assert "0".equals(stats.get("bytes_read"));
      assert "0".equals(stats.get("bytes_written"));
      assert "0".equals(stats.get("threads"));
   }

   public void testStats(Method m) throws Exception {
      Map<String, String> stats = getStats();
      assert "0".equals(stats.get("cmd_set"));
      assert "0".equals(stats.get("cmd_get"));
      assert "0".equals(stats.get("get_hits"));
      assert "0".equals(stats.get("get_misses"));
      assert "0".equals(stats.get("delete_hits"));
      assert "0".equals(stats.get("delete_misses"));
      assert "0".equals(stats.get("curr_items"));
      assert "0".equals(stats.get("total_items"));
      assert "0".equals(stats.get("incr_misses"));
      assert "0".equals(stats.get("incr_hits"));
      assert "0".equals(stats.get("decr_misses"));
      assert "0".equals(stats.get("decr_hits"));
      assert "0".equals(stats.get("cas_misses"));
      assert "0".equals(stats.get("cas_hits"));
      assert "0".equals(stats.get("cas_badval"));

      Future<Boolean> f = client.set(k(m), 0, v(m));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m).equals(client.get(k(m)));

      f = client.set(k(m, "k1-"), 0, v(m, "v1-"));
      assert f.get(5, TimeUnit.SECONDS);
      assert v(m, "v1-").equals(client.get(k(m, "k1-")));

      stats = getStats();
      assert "2".equals(stats.get("cmd_set"));
      assert "4".equals(stats.get("cmd_get"));
      assert "2".equals(stats.get("get_hits"));
      assert "2".equals(stats.get("get_misses"));
      assert "0".equals(stats.get("delete_hits"));
      assert "0".equals(stats.get("delete_misses"));
      assert "2".equals(stats.get("curr_items"));
      assert "2".equals(stats.get("total_items"));

      f = client.delete(k(m, "k1-"));
      assert f.get(5, TimeUnit.SECONDS);
      stats = getStats();
      assert "1".equals(stats.get("curr_items"));
      assert "2".equals(stats.get("total_items"));
      assert "1".equals(stats.get("delete_hits"));
      assert "0".equals(stats.get("delete_misses"));

      assert null == client.get(k(m, "k99-"));
      stats = getStats();
      assert "2".equals(stats.get("get_hits"));
      assert "3".equals(stats.get("get_misses"));

      f = client.delete(k(m, "k99-"));
      assert !f.get(5, TimeUnit.SECONDS);
      stats = getStats();
      assert "1".equals(stats.get("delete_hits"));
      assert "1".equals(stats.get("delete_misses"));

      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      f = client.set(k(m, "k3-"), future, v(m, "v3-"));
      assert f.get(5, TimeUnit.SECONDS);
      Thread.sleep(1100);
      assert null == client.get(k(m, "k3-"));
      stats = getStats();
      assert "1".equals(stats.get("curr_items"));
      assert "3".equals(stats.get("total_items"));

      client.incr(k(m, "k4-"), 1);
      stats = getStats();
      assert "1".equals(stats.get("incr_misses"));
      assert "0".equals(stats.get("incr_hits"));

      f = client.set(k(m, "k4-"), 0, 1);
      assert f.get(5, TimeUnit.SECONDS);
      client.incr(k(m, "k4-"), 1);
      client.incr(k(m, "k4-"), 2);
      client.incr(k(m, "k4-"), 4);
      stats = getStats();
      assert "1".equals(stats.get("incr_misses"));
      assert "3".equals(stats.get("incr_hits"));

      client.decr(k(m, "k5-"), 1);
      stats = getStats();
      assert "1".equals(stats.get("decr_misses"));
      assert "0".equals(stats.get("decr_hits"));
      
      f = client.set(k(m, "k5-"), 0, 8);
      assert f.get(5, TimeUnit.SECONDS);
      client.decr(k(m, "k5-"), 1);
      client.decr(k(m, "k5-"), 2);
      client.decr(k(m, "k5-"), 4);
      stats = getStats();
      assert "1".equals(stats.get("decr_misses"));
      assert "3".equals(stats.get("decr_hits"));

      client.cas(k(m, "k6-"), 1234, v(m, "v6-"));
      stats = getStats();
      assert "1".equals(stats.get("cas_misses"));
      assert "0".equals(stats.get("cas_hits"));
      assert "0".equals(stats.get("cas_badval"));

      f = client.set(k(m, "k6-"), 0, v(m, "v6-"));
      assert f.get(5, TimeUnit.SECONDS);
      CASValue<Object> value = client.gets(k(m, "k6-"));
      long old = value.getCas();
      client.cas(k(m, "k6-"), value.getCas(), v(m, "v66-"));
      stats = getStats();
      assert "1".equals(stats.get("cas_misses"));
      assert "1".equals(stats.get("cas_hits"));
      assert "0".equals(stats.get("cas_badval"));

      client.cas(k(m, "k6-"), old, v(m, "v66-"));
      stats = getStats();
      assert "1".equals(stats.get("cas_misses"));
      assert "1".equals(stats.get("cas_hits"));
      assert "1".equals(stats.get("cas_badval"));
   }

   private Map<String, String> getStats() {
      Map<SocketAddress, Map<String, String>> stats = client.getStats();
      assert 1 == stats.size();
      return stats.values().iterator().next();
   }
}
