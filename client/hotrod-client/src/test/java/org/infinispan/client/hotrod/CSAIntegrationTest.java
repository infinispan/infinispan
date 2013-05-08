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
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import static org.infinispan.test.TestingUtil.*;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.CSAIntegrationTest")
public class CSAIntegrationTest extends HitsAwareCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private TcpTransportFactory tcpConnectionFactory;

   private static final Log log = LogFactory.getLog(CSAIntegrationTest.class);

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      builder.unsafe().unreliableReturnValues(true);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hrServ2CacheManager.put(new InetSocketAddress(hotRodServer1.getHost(), hotRodServer1.getPort()), manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hrServ2CacheManager.put(new InetSocketAddress(hotRodServer2.getHost(), hotRodServer2.getPort()), manager(1));
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      hrServ2CacheManager.put(new InetSocketAddress(hotRodServer3.getHost(), hotRodServer3.getPort()), manager(2));

      assert manager(0).getCache() != null;
      assert manager(1).getCache() != null;
      assert manager(2).getCache() != null;

      blockUntilViewReceived(manager(0).getCache(), 3);
      blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);
      blockUntilCacheStatusAchieved(manager(2).getCache(), ComponentStatus.RUNNING, 10000);

      //Important: this only connects to one of the two servers!
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      props.put("infinispan.client.hotrod.ping_on_startup", "true");
      setHotRodProtocolVersion(props);
      remoteCacheManager = new RemoteCacheManager(props);
      remoteCache = remoteCacheManager.getCache();

      tcpConnectionFactory = (TcpTransportFactory) extractField(remoteCacheManager, "transportFactory");
   }

   protected void setHotRodProtocolVersion(Properties props) {
      // No-op, use default Hot Rod protocol version
   }

   @AfterClass
   @Override
   protected void destroy() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer1, hotRodServer2, hotRodServer3);
      super.destroy();
   }

   public void testHashInfoRetrieved() throws InterruptedException {
      assertEquals(3, tcpConnectionFactory.getServers().size());
      for (int i = 0; i < 10; i++) {
         remoteCache.put("k", "v");
         if (tcpConnectionFactory.getServers().size() == 3) break;
         Thread.sleep(1000);
      }
      assertEquals(3, tcpConnectionFactory.getServers().size());
      assertNotNull(tcpConnectionFactory.getConsistentHash());
   }

   @Test(dependsOnMethods = "testHashInfoRetrieved")
   public void testCorrectSetup() {
      remoteCache.put("k", "v");
      assert remoteCache.get("k").equals("v");
   }

   @Test(dependsOnMethods = "testCorrectSetup")
   public void testHashFunctionReturnsSameValues() {
      for (int i = 0; i < 1000; i++) {
         byte[] key = generateKey(i);
         TcpTransport transport = (TcpTransport) tcpConnectionFactory.getTransport(key);
         SocketAddress serverAddress = transport.getServerAddress();
         CacheContainer cacheContainer = hrServ2CacheManager.get(serverAddress);
         assertNotNull("For server address " + serverAddress + " found " + cacheContainer + ". Map is: " + hrServ2CacheManager, cacheContainer);
         DistributionManager distributionManager = cacheContainer.getCache().getAdvancedCache().getDistributionManager();
         Address clusterAddress = cacheContainer.getCache().getAdvancedCache().getRpcManager().getAddress();

         ConsistentHash serverCh = distributionManager.getReadConsistentHash();
         int numSegments = serverCh.getNumSegments();
         int keySegment = serverCh.getSegment(key);
         Address serverOwner = serverCh.locatePrimaryOwnerForSegment(keySegment);
         Address serverPreviousOwner = serverCh.locatePrimaryOwnerForSegment((keySegment - 1 + numSegments) % numSegments);
         assert clusterAddress.equals(serverOwner) || clusterAddress.equals(serverPreviousOwner);
         tcpConnectionFactory.releaseTransport(transport);
      }
   }

   @Test(dependsOnMethods = "testHashFunctionReturnsSameValues")
   public void testRequestsGoToExpectedServer() throws Exception {
      addInterceptors();
      List<byte[]> keys = new ArrayList<byte[]>();
      for (int i = 0; i < 500; i++) {
         byte[] key = generateKey(i);
         keys.add(key);
         String keyStr = new String(key);
         remoteCache.put(keyStr, "value");
         TcpTransport transport = (TcpTransport) tcpConnectionFactory.getTransport(marshall(keyStr));
         assertHotRodEquals(hrServ2CacheManager.get(transport.getServerAddress()), keyStr, "value");
         tcpConnectionFactory.releaseTransport(transport);
      }

      log.info("Right before first get.");

      for (byte[] key : keys) {
         resetStats();
         String keyStr = new String(key);
         assert remoteCache.get(keyStr).equals("value");
         TcpTransport transport = (TcpTransport) tcpConnectionFactory.getTransport(marshall(keyStr));
         assertOnlyServerHit(transport.getServerAddress());
         tcpConnectionFactory.releaseTransport(transport);
      }
   }

   private void assertCacheContainsKey(SocketAddress serverAddress, byte[] keyBytes) {
      CacheContainer cacheContainer = hrServ2CacheManager.get(serverAddress);
      Cache<Object, Object> cache = cacheContainer.getCache();
      DataContainer dataContainer = cache.getAdvancedCache().getDataContainer();
      assert dataContainer.keySet().contains(new ByteArrayKey(keyBytes));
   }

   private byte[] generateKey(int i) {
      Random r = new Random();
      byte[] result = new byte[i];
      r.nextBytes(result);
      return result;
   }
}
