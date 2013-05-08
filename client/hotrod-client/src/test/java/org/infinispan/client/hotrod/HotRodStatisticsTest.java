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

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.HotRodStatisticsTest")
public class HotRodStatisticsTest {

   private HotRodServer hotrodServer;
   private CacheContainer cacheContainer;
   private RemoteCacheManager rcm;
   private RemoteCache remoteCache;
   long startTime;

   @BeforeMethod
   protected void setup() throws Exception {
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cfg.jmxStatistics().enable();
      cacheContainer = TestCacheManagerFactory
            .createClusteredCacheManagerEnforceJmxDomain(getClass().getSimpleName(), cfg);

      hotrodServer = TestHelper.startHotRodServer((EmbeddedCacheManager) cacheContainer);
      startTime = System.currentTimeMillis();
      rcm = new RemoteCacheManager("localhost", hotrodServer.getPort());
      remoteCache = rcm.getCache();
   }

   @AfterMethod
   void tearDown() {
      TestingUtil.killCacheManagers(cacheContainer);
      killRemoteCacheManager(rcm);
      killServers(hotrodServer);
   }

   public void testAllStatsArePresent() {
      ServerStatistics serverStatistics = remoteCache.stats();
      Map<String, String> statsMap = serverStatistics.getStatsMap();
      assertEquals(statsMap.get(ServerStatistics.STORES), "0");
      assertEquals(statsMap.get(ServerStatistics.CURRENT_NR_OF_ENTRIES), "0");
      assertEquals(statsMap.get(ServerStatistics.HITS),"0");
      assertEquals(statsMap.get(ServerStatistics.MISSES),"0");
      assertEquals(statsMap.get(ServerStatistics.REMOVE_HITS),"0");
      assertEquals(statsMap.get(ServerStatistics.REMOVE_MISSES),"0");
      assertEquals(statsMap.get(ServerStatistics.RETRIEVALS),"0");
      assertEquals(statsMap.get(ServerStatistics.TOTAL_NR_OF_ENTRIES),"0");
      assert 0 == remoteCache.size();
      assert remoteCache.isEmpty();

      Integer number = serverStatistics.getIntStatistic(ServerStatistics.TIME_SINCE_START);
      assertTrue(number >= 0);
   }

   public void testStoresAndEntries() {
      assert 0 == remoteCache.size();
      assert remoteCache.isEmpty();
      remoteCache.put("a","v");
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.STORES));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assert 1 == remoteCache.size();
      assert !remoteCache.isEmpty();
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
      remoteCache.put("a2","v2");
      assertEquals((Integer)2, remoteCache.stats().getIntStatistic(ServerStatistics.STORES));
      assertEquals((Integer)2, remoteCache.stats().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals((Integer)2, remoteCache.stats().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
      assert 2 == remoteCache.size();
      assert !remoteCache.isEmpty();
      remoteCache.put("a2","v3");
      assertEquals((Integer)3, remoteCache.stats().getIntStatistic(ServerStatistics.STORES));
      assertEquals((Integer)2, remoteCache.stats().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals((Integer)3, remoteCache.stats().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
      assert 2 == remoteCache.size();
      assert !remoteCache.isEmpty();
   }

   public void testHitsAndMisses() {
      remoteCache.get("a");
      assertEquals((Integer)0, remoteCache.stats().getIntStatistic(ServerStatistics.HITS));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.MISSES));
      remoteCache.put("a","v");
      assertEquals((Integer)0, remoteCache.stats().getIntStatistic(ServerStatistics.HITS));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.MISSES));
      remoteCache.get("a");
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.HITS));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.MISSES));
      remoteCache.get("a");
      remoteCache.get("a");
      remoteCache.get("a");
      assertEquals((Integer)4, remoteCache.stats().getIntStatistic(ServerStatistics.HITS));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.MISSES));
   }

   public void testRemoveHitsAndMisses() {
      remoteCache.remove("a");
      assertEquals((Integer)0, remoteCache.stats().getIntStatistic(ServerStatistics.REMOVE_HITS));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.REMOVE_MISSES));
      remoteCache.put("a","v");
      remoteCache.remove("a");
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.REMOVE_HITS));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.REMOVE_MISSES));
      remoteCache.put("a","v");
      remoteCache.put("b","v");
      remoteCache.put("c","v");

      remoteCache.remove("a");
      remoteCache.remove("b");
      remoteCache.remove("c");
      assertEquals((Integer)4, remoteCache.stats().getIntStatistic(ServerStatistics.REMOVE_HITS));
      assertEquals((Integer)1, remoteCache.stats().getIntStatistic(ServerStatistics.REMOVE_MISSES));
   }

   public void testNumberOfEntriesAfterClear() {
      assertEquals((Integer)0, remoteCache.stats().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      remoteCache.put("k", "v");
      remoteCache.put("k2", "v");
      assertEquals((Integer)2, remoteCache.stats().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      remoteCache.clear();
      assertEquals((Integer)0, remoteCache.stats().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals((Integer)2, remoteCache.stats().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
   }

}
