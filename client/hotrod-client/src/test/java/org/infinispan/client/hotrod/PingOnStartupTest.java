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
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.PingOnStartupTest")
public class PingOnStartupTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(CacheMode.DIST_SYNC);
      createHotRodServers(2, config);
   }

   public void testTopologyFetched() throws Exception {
      Properties props = new Properties();
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "true");
      props.put("timeBetweenEvictionRunsMillis", "500");
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory)
            TestingUtil.extractField(remoteCacheManager, "transportFactory");
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
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "false");
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory)
            TestingUtil.extractField(remoteCacheManager, "transportFactory");
      try {
         assertEquals(1, tcpConnectionFactory.getServers().size());
      } finally {
         remoteCacheManager.stop();
      }
   }

   public void testGetCacheWithPingOnStartupDisabled() {
      Properties props = new Properties();
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "boomoo:12345;localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "false");
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);

      try {
         RemoteCache<Object, Object> cache = remoteCacheManager.getCache();
         assertFalse(cache.containsKey("k"));
      } finally {
         remoteCacheManager.stop();
      }
   }

}
