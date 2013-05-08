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

import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ReplTopologyChangeTest", groups = "functional")
public class ReplTopologyChangeTest extends MultipleCacheManagersTest {

   HotRodServer hotRodServer1;
   HotRodServer hotRodServer2;
   HotRodServer hotRodServer3 ;

   RemoteCache remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private TcpTransportFactory tcpConnectionFactory;
   private ConfigurationBuilder config;

   @Override
   protected void assertSupportedConfig() {
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @AfterClass
   @Override
   protected void destroy() {
      super.destroy();
      killServers(hotRodServer1, hotRodServer2);
      killRemoteCacheManager(remoteCacheManager);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(getCacheMode(), false));
      CacheContainer cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      waitForClusterToForm();
   }

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass(); // Create cache managers
      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));

      //Important: this only connects to one of the two servers!
      remoteCacheManager = new RemoteCacheManager("localhost", hotRodServer2.getPort());
      remoteCache = remoteCacheManager.getCache();

      tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public void testTwoMembers() {
      InetSocketAddress server1Address = new InetSocketAddress("localhost", hotRodServer1.getPort());
      expectTopologyChange(server1Address, true);
      assertEquals(2, tcpConnectionFactory.getServers().size());
   }

   @Test(dependsOnMethods = "testTwoMembers")
   public void testAddNewServer() {
      CacheContainer cm3 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm3);
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      manager(2).getCache();

      waitForClusterToForm();

      try {
         expectTopologyChange(new InetSocketAddress("localhost", hotRodServer3.getPort()), true);
         assertEquals(3, tcpConnectionFactory.getServers().size());
      } finally {
         log.info("Members are: " + manager(0).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(1).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(2).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
      }
   }

   @Test(dependsOnMethods = "testAddNewServer")
   public void testDropServer() {
      hotRodServer3.stop();
      manager(2).stop();
      log.trace("Just stopped server 2");

      waitForServerToDie(2);

      InetSocketAddress server3Address = new InetSocketAddress("localhost", hotRodServer3.getPort());      

      try {
         expectTopologyChange(server3Address, false);
         assertEquals(2, tcpConnectionFactory.getServers().size());
      } finally {
         log.info("Members are: " + manager(0).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(1).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         if (manager(2).getStatus() != ComponentStatus.RUNNING)
            log.info("Members are: 0");
         else
            log.info("Members are: " + manager(2).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
      }
   }

   private void expectTopologyChange(InetSocketAddress server1Address, boolean added) {
      for (int i = 0; i < 10; i++) {
         remoteCache.put("k" + i, "v" + i);         
         if (added == tcpConnectionFactory.getServers().contains(server1Address)) break;
      }
      Collection<SocketAddress> addresses = tcpConnectionFactory.getServers();
      assertEquals(server1Address + " not found in " + addresses, added, addresses.contains(server1Address));
   }
   
   protected void waitForServerToDie(int memberCount) {
      TestingUtil.blockUntilViewReceived(manager(0).getCache(), memberCount, 30000, false);
   }
}
