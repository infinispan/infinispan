/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.retry.DistributionRetryTest;
import org.infinispan.config.Configuration;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.Util;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Mircea Markus
 */
@Test (groups = "functional", testName = "client.hotrod.ConsistentHashV1IntegrationTest", enabled = false, description = "See ISPN-1123")
public class ConsistentHashV1IntegrationTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private HotRodServer hotRodServer4; //tod add shutdown behaviour
   private RemoteCacheManager remoteCacheManager;
   private RemoteCacheImpl remoteCache;
   private KeyAffinityService kas;
   private ExecutorService ex;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration conf = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      conf.fluent().jmxStatistics();
      assert conf.isExposeJmxStatistics();
      conf.fluent().hash().numOwners(2);
      conf.fluent().hash().rehashEnabled(false);

      addClusterEnabledCacheManager(conf, true);
      addClusterEnabledCacheManager(conf, true);
      addClusterEnabledCacheManager(conf, true);
      addClusterEnabledCacheManager(conf, true);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      hotRodServer4 = TestHelper.startHotRodServer(manager(3));


      waitForClusterToForm();

      Properties clientConfig = new Properties();
      clientConfig.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer2.getPort());

      remoteCacheManager = new RemoteCacheManager(clientConfig);
      remoteCache = (RemoteCacheImpl) remoteCacheManager.getCache();
      assert super.cacheManagers.size() == 4;

      ex = Executors.newSingleThreadExecutor();
      kas = KeyAffinityServiceFactory.newKeyAffinityService(cache(0),
                                                            ex,
                                                            new DistributionRetryTest.ByteKeyGenerator(),
                                                            2, true);

      for (int i = 0; i < 4; i++) {
         advancedCache(i).addInterceptor(new HitsAwareCacheManagersTest.HitCountInterceptor(), 1);
      }
   }

   @AfterTest
   public void cleanUp() {
      ex.shutdownNow();
      kas.stop();

      stopServer(hotRodServer1);
      stopServer(hotRodServer2);
      stopServer(hotRodServer3);
      stopServer(hotRodServer4);

      remoteCache.stop();
      remoteCacheManager.stop();
   }

   private void stopServer(HotRodServer hrs) {
      try {
         hrs.stop();
      } catch (Exception e) {
         //ignore
      }
   }

   public void testCorrectBalancingOfKeys() {
      runTest(0);
      runTest(1);
      runTest(2);
      runTest(3);
   }

   private void runTest(int cacheIndex) {
      List<Address> backups = advancedCache(cacheIndex).getDistributionManager().getConsistentHash().locate(address(cacheIndex), 2);
      assert backups.contains(address(cacheIndex));
      Map<Address, Integer> hitNodes = new HashMap<Address, Integer>();
      hitNodes.put(backups.get(0), 0);
      hitNodes.put(backups.get(1), 0);

      for (int i = 0; i < 1000; i++) {
         String key = getKey(cacheIndex);
         remoteCache.put(key, "v");
         Address hitServer = getHitServer();
         assert backups.contains(hitServer) : String.format("i=%s, backups: %s, hit server: %s, key=%s", i, backups, hitServer, Util.printArray(key.getBytes(), false));
         hitNodes.put(hitServer, hitNodes.get(hitServer) + 1);
      }
      System.out.println("hitNodes = " + hitNodes);
      assert backups.containsAll(hitNodes.keySet()) : String.format("Backups %s. hit nodes %s", backups, hitNodes);
   }

   private String getKey(int cacheIndex) {
      byte[] keyBytes = (byte[]) kas.getKeyForAddress(address(cacheIndex));
      return DistributionRetryTest.ByteKeyGenerator.getStringObject(keyBytes);
   }

   private Address getHitServer() {
      List<Address> result = new ArrayList<Address>();
      for (int i = 0; i < 4; i++) {
         InterceptorChain ic = advancedCache(i).getComponentRegistry().getComponent(InterceptorChain.class);
         HitsAwareCacheManagersTest.HitCountInterceptor interceptor =
               (HitsAwareCacheManagersTest.HitCountInterceptor) ic.getInterceptorsWithClassName(HitsAwareCacheManagersTest.HitCountInterceptor.class.getName()).get(0);
         if (interceptor.getHits() == 1) {
            result.add(address(i));
         }
         interceptor.reset();
      }
      if (result.size() > 1) throw new IllegalStateException("More than one hit! : " + result);
      return result.get(0);
   }
}
