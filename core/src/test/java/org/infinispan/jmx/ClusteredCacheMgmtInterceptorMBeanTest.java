/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.jmx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.HashMap;
import java.util.Map;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "jmx.ClusteredCacheMgmtInterceptorMBeanTest")
public class ClusteredCacheMgmtInterceptorMBeanTest extends MultipleCacheManagersTest {

   private static final String JMX_1 = ClusteredCacheMgmtInterceptorMBeanTest.class.getSimpleName() + "-1";
   private static final String JMX_2 = ClusteredCacheMgmtInterceptorMBeanTest.class.getSimpleName() + "-2";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(
            CacheMode.REPL_SYNC, false);
      builder.jmxStatistics().enable();

      EmbeddedCacheManager cm1 = TestCacheManagerFactory
            .createClusteredCacheManagerEnforceJmxDomain(JMX_1, builder);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory
            .createClusteredCacheManagerEnforceJmxDomain(JMX_2, builder);
      registerCacheManager(cm1, cm2);
   }

   public void testCorrectStatsInCluster() throws Exception {
      Cache<String, String> cache1 = cache(0);
      Cache<String, String> cache2 = cache(1);
      cache1.put("k", "v");
      assertEquals("v", cache2.get("k"));
      ObjectName stats1 = getCacheObjectName(JMX_1, "___defaultcache(repl_sync)", "Statistics");
      ObjectName stats2 = getCacheObjectName(JMX_2, "___defaultcache(repl_sync)", "Statistics");

      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      assertEquals((long) 1, mBeanServer.getAttribute(stats1, "Stores"));
      assertEquals((long) 0, mBeanServer.getAttribute(stats2, "Stores"));

      Map<String, String> values = new HashMap<String, String>();
      values.put("k1", "v1");
      values.put("k2", "v2");
      values.put("k3", "v3");
      cache2.putAll(values);

      assertEquals((long) 1, mBeanServer.getAttribute(stats1, "Stores"));
      assertEquals((long) 3, mBeanServer.getAttribute(stats2, "Stores"));

      cache1.remove("k");

      assertEquals((long) 1, mBeanServer.getAttribute(stats1, "RemoveHits"));
      assertEquals((long) 0, mBeanServer.getAttribute(stats2, "RemoveHits"));
   }

}
