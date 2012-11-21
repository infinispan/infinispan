/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.query.jmx;

import org.infinispan.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.jmx.QueryMBeanTest")
public class QueryMBeanTest extends SingleCacheManagerTest {

   static final String JMX_DOMAIN = QueryMBeanTest.class.getSimpleName();
   static final String CACHE_NAME = "queryable-cache";
   MBeanServer server;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm =
            TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(JMX_DOMAIN);

      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.indexing().enable().indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      cm.defineConfiguration(CACHE_NAME, builder.build());
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
   }

   public void testQueryStatsMBean() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start cache
      ObjectName name = getQueryStatsObjectName(JMX_DOMAIN, CACHE_NAME);
      assert server.isRegistered(name);
      assert !(Boolean) server.getAttribute(name, "StatisticsEnabled");
      server.setAttribute(name, new Attribute("StatisticsEnabled", true));
      assert (Boolean) server.getAttribute(name, "StatisticsEnabled");
   }

   ObjectName getQueryStatsObjectName(String jmxDomain, String cacheName) {
      try {
         return new ObjectName(jmxDomain + ":type=Query,name="
               + ObjectName.quote(cacheName) + ",component=Statistics");
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Malformed object name", e);
      }
   }

}
