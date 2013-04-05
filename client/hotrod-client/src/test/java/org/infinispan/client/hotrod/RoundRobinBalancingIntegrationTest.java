/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.RoundRobinBalancingIntegrationTest", groups="functional")
public class RoundRobinBalancingIntegrationTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(RoundRobinBalancingIntegrationTest.class);

   Cache c1;
   Cache c2;
   Cache c3;
   Cache c4;

   HotRodServer hotRodServer1;
   HotRodServer hotRodServer2;

   HotRodServer hotRodServer3;
   HotRodServer hotRodServer4;

   RemoteCache<String, String> remoteCache;

   private RemoteCacheManager remoteCacheManager;

   @Override
   protected void assertSupportedConfig() {
      return;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      c1 = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration()).getCache();
      c2 = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration()).getCache();
      c3 = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration()).getCache();
      registerCacheManager(c1.getCacheManager(), c2.getCacheManager(), c3.getCacheManager());

      hotRodServer1 = TestHelper.startHotRodServer(c1.getCacheManager());
      hotRodServer2 = TestHelper.startHotRodServer(c2.getCacheManager());
      hotRodServer3 = TestHelper.startHotRodServer(c3.getCacheManager());

      log.trace("Server 1 port: " + hotRodServer1.getPort());
      log.trace("Server 2 port: " + hotRodServer2.getPort());
      log.trace("Server 3 port: " + hotRodServer3.getPort());
      String servers = TestHelper.getServersString(hotRodServer1, hotRodServer2, hotRodServer3);
      log.trace("Server list is: " + servers);
      remoteCacheManager = new RemoteCacheManager(servers);
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterTest(alwaysRun = true)
   public void tearDown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer1, hotRodServer2, hotRodServer3, hotRodServer4);
   }

   public void testRoundRobinLoadBalancing() {
      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");

      assertEquals(1, c1.size());
      assertEquals(1, c2.size());
      assertEquals(1, c3.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));

      remoteCache.put("k4", "v1");
      remoteCache.put("k5", "v2");
      remoteCache.put("k6", "v3");
      remoteCache.put("k7", "v1");
      remoteCache.put("k8", "v2");
      remoteCache.put("k9", "v3");

      assertEquals(3, c1.size());
      assertEquals(3, c2.size());
      assertEquals(3, c3.size());
   }

   @Test(dependsOnMethods = "testRoundRobinLoadBalancing")
   public void testAddNewHotrodServer() {
      c4 = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration()).getCache();
      hotRodServer4 = TestHelper.startHotRodServer(c4.getCacheManager());
      registerCacheManager(c4.getCacheManager());

      List<SocketAddress> serverAddresses = new ArrayList<SocketAddress>();
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer1.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer2.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer3.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer4.getPort()));

      RoundRobinBalancingStrategy balancer = getBalancer();
      balancer.setServers(serverAddresses);

      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");
      remoteCache.put("k4", "v4");

      assertEquals(1, c1.size());
      assertEquals(1, c2.size());
      assertEquals(1, c3.size());
      assertEquals(1, c4.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));
      assertEquals("v4", remoteCache.get("k4"));

      remoteCache.put("k5", "v2");
      remoteCache.put("k6", "v3");
      remoteCache.put("k7", "v1");
      remoteCache.put("k8", "v2");
      remoteCache.put("k9", "v3");
      remoteCache.put("k10", "v3");
      remoteCache.put("k11", "v3");
      remoteCache.put("k12", "v3");

      assertEquals(3, c1.size());
      assertEquals(3, c2.size());
      assertEquals(3, c3.size());
      assertEquals(3, c4.size());
   }

   @Test(dependsOnMethods = "testAddNewHotrodServer")
   public void testStopServer() {
      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");
      remoteCache.put("k4", "v4");

      assertEquals(1, c1.size());
      assertEquals(1, c2.size());
      assertEquals(1, c3.size());
      assertEquals(1, c4.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));
      assertEquals("v4", remoteCache.get("k4"));

      hotRodServer4.stop();

      try {
         remoteCache.put("k5", "v1");
         remoteCache.put("k6", "v2");
         remoteCache.put("k7", "v3");
         remoteCache.put("k8", "v4");
      } catch (Exception e) {
         assert false : "exception should not happen even if the balancer redirects to failed node at the beggining";
      }
   }

   @Test(dependsOnMethods = "testStopServer")
   public void testRemoveServers() {
      List<SocketAddress> serverAddresses = new ArrayList<SocketAddress>();
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer1.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer2.getPort()));

      RoundRobinBalancingStrategy balancer = getBalancer();
      balancer.setServers(serverAddresses);

      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");
      remoteCache.put("k4", "v4");

      assertEquals(2, c1.size());
      assertEquals(2, c2.size());
      assertEquals(0, c3.size());
      assertEquals(0, c4.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));
      assertEquals("v4", remoteCache.get("k4"));

      remoteCache.put("k5", "v2");
      remoteCache.put("k6", "v3");
      remoteCache.put("k7", "v1");
      remoteCache.put("k8", "v2");
      remoteCache.put("k9", "v3");
      remoteCache.put("k10", "v3");
      remoteCache.put("k11", "v3");
      remoteCache.put("k12", "v3");

      assertEquals(6, c1.size());
      assertEquals(6, c2.size());
      assertEquals(0, c3.size());
      assertEquals(0, c4.size());

   }

   private RoundRobinBalancingStrategy getBalancer() {
      TcpTransportFactory transportFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCache.getRemoteCacheManager(), "transportFactory");
      return (RoundRobinBalancingStrategy) TestingUtil.extractField(transportFactory, "balancer");
   }

}
