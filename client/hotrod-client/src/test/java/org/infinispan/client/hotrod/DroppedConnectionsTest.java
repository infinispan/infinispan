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

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.DroppedConnectionsTest", groups = "functional")
public class DroppedConnectionsTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache rc;
   private TcpTransportFactory transportFactory;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(getDefaultStandaloneCacheConfig(false)));
      hotRodServer = TestHelper.startHotRodServer(cacheManager);
      Properties hrClientConfig = new Properties();
      hrClientConfig.put("testWhileIdle", "false");
      hrClientConfig.put("minIdle","1");
      hrClientConfig.put("maxIdle","2");
      hrClientConfig.put("maxActive","2");
      hrClientConfig.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(hrClientConfig);
      rc = remoteCacheManager.getCache();
      transportFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      return cacheManager;
   }

   @AfterClass
   @Override
   protected void teardown() {
      super.teardown();
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotRodServer);
   }

   public void testClosedConnection() throws Exception {
      rc.put("k","v"); //make sure a connection is created

      GenericKeyedObjectPool keyedObjectPool = transportFactory.getConnectionPool();
      InetSocketAddress address = new InetSocketAddress("127.0.0.1", hotRodServer.getPort());

      assertEquals(0, keyedObjectPool.getNumActive(address));
      assertEquals(1, keyedObjectPool.getNumIdle(address));

      TcpTransport tcpConnection = (TcpTransport) keyedObjectPool.borrowObject(address);
      keyedObjectPool.returnObject(address, tcpConnection);//now we have a reference to the single connection in pool

      tcpConnection.destroy();

      assertEquals("v", rc.get("k"));
      assertEquals(0, keyedObjectPool.getNumActive(address));
      assertEquals(1, keyedObjectPool.getNumIdle(address));

      TcpTransport tcpConnection2 = (TcpTransport) keyedObjectPool.borrowObject(address);

      assert tcpConnection2.getId() != tcpConnection.getId();
   }

}
