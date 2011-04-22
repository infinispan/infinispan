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
import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.PingOnStartupTest")
public class PingOnStartupTest extends MultipleCacheManagersTest {
   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      addClusterEnabledCacheManager(config);
      addClusterEnabledCacheManager(config);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));

      assert manager(0).getCache() != null;
      assert manager(1).getCache() != null;

      TestingUtil.blockUntilViewReceived(manager(0).getCache(), 2, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);

      cache(0).put("k","v");
      assertEquals("v", cache(1).get("k"));
   }

   @AfterClass
   @Override
   protected void destroy() {
      super.destroy();
      try {
         hotRodServer1.stop();
         hotRodServer2.stop();
      } catch (Exception e) {
         //ignore
      }
   }

   public void testTopologyFetched() throws Exception {
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "true");
      props.put("timeBetweenEvictionRunsMillis", "500");
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      for (int i = 0; i < 10; i++) {
         try {
            if (tcpConnectionFactory.getServers().size() == 1) {
               Thread.sleep(1000);
            } else {
               break;
            }
         } finally {
            remoteCacheManager.stop();
         }
      }
      assertEquals(2, tcpConnectionFactory.getServers().size());
   }

   public void testTopologyNotFetched() {
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "false");
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      try {
         assertEquals(1, tcpConnectionFactory.getServers().size());
      } finally {
         remoteCacheManager.stop();
      }
   }
}
