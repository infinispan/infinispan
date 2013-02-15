/*
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.ServerAddress;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteArrayKey;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;

@Test(groups = "functional" , testName = "client.hotrod.LocateAPIDistTest")
public class LocateAPIDistTest extends MultipleCacheManagersTest {

   HotRodServer hotRodServer1;
   HotRodServer hotRodServer2;
   HotRodServer hotRodServer3;
   HotRodServer hotRodServer4;

   RemoteCache remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private TcpTransportFactory tcpConnectionFactory;

   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(getCacheMode(), false);
      CacheContainer cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm3 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm4 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      registerCacheManager(cm3);
      registerCacheManager(cm4);
      waitForClusterToForm();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      killServers(hotRodServer1, hotRodServer2, hotRodServer3, hotRodServer4);
      killRemoteCacheManager(remoteCacheManager);
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass(); // Create cache managers
      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      hotRodServer4 = TestHelper.startHotRodServer(manager(3));

      //Important: this only connects to one of the two servers!
      remoteCacheManager = new RemoteCacheManager("localhost", hotRodServer1.getPort());
      remoteCache = remoteCacheManager.getCache();

      tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
   }

   public void testLocate() {
      remoteCache.put("key", "value");
      remoteCache.get("key");

      RemoteCacheImpl impl = (RemoteCacheImpl) remoteCache;
      ByteArrayKey keyOnServer = new ByteArrayKey(impl.obj2bytes("key", true));

      List<Address> owners = cache(0).getAdvancedCache().getDistributionManager().locate(keyOnServer);
      Collection<String> ownersOnClient = remoteCache.locate("key");

      Set<String> ownerEndpoints = new HashSet<String>();
      for (Address a: owners) {
         for (HotRodServer hrs: Arrays.asList(hotRodServer1, hotRodServer2, hotRodServer3, hotRodServer4)) {
            if (hrs.getCacheManager().getAddress().equals(a)) {
               ownerEndpoints.add("/" + hrs.getAddress()); // For some reason there is no leading / in the ServerEndpoint's toString()
            }
         }
      }

      assert ownerEndpoints.equals(ownersOnClient) : "Client's locate() method returned different results from the server's locate()!";
   }
}
