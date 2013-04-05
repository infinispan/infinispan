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
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests ping-on-startup logic whose objective is to retrieve the Hot Rod
 * server topology before a client executes an operation against the server.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.PingOnStartupTest")
public class PingOnStartupTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      createHotRodServers(2, builder);
   }

   public void testTopologyFetched() {
      Properties props = new Properties();
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "true");

      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(props)) {
         @Override
         public void call() {
            TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory)
                  TestingUtil.extractField(rcm, "transportFactory");
            for (int i = 0; i < 10; i++) {
               if (tcpConnectionFactory.getServers().size() == 1) {
                  TestingUtil.sleepThread(1000);
               } else {
                  break;
               }
            }
            assertEquals(2, tcpConnectionFactory.getServers().size());
         }
      });
   }

   public void testTopologyNotFetched() {
      Properties props = new Properties();
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "false");

      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(props)) {
         @Override
         public void call() {
            TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory)
                  TestingUtil.extractField(rcm, "transportFactory");
            assertEquals(1, tcpConnectionFactory.getServers().size());
         }
      });
   }

   public void testGetCacheWithPingOnStartupDisabledSingleNode() {
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", "boomoo:12345");
      props.put("infinispan.client.hotrod.ping_on_startup", "false");

      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(props)) {
         @Override
         public void call() {
            rcm.getCache();
         }
      });
   }

   public void testGetCacheWithPingOnStartupDisabledMultipleNodes() {
      Properties props = new Properties();
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "boomoo:12345;localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "false");

      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(props)) {
         @Override
         public void call() {
            RemoteCache<Object, Object> cache = rcm.getCache();
            assertFalse(cache.containsKey("k"));
         }
      });
   }

   public void testGetCacheWorksIfNodeDown() {
      Properties props = new Properties();
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "boomoo:12345;localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "true");

      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(props)) {
         @Override
         public void call() {
            rcm.getCache();
         }
      });
   }

   public void testGetCacheWorksIfNodeNotDown() {
      Properties props = new Properties();
      HotRodServer hotRodServer2 = server(1);
      props.put("infinispan.client.hotrod.server_list",
            "localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "true");
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(props)) {
         @Override
         public void call() {
            rcm.getCache();
         }
      });
   }

}
