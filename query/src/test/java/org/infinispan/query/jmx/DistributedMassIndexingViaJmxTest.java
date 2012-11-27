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

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.distributed.DistributedMassIndexingTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.FileLookupFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.InputStream;

/**
 * Test reindexing happens when executed via JMX
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.jmx.DistributedMassIndexingViaJmxTest")
public class DistributedMassIndexingViaJmxTest extends DistributedMassIndexingTest {

   static final String BASE_JMX_DOMAIN = DistributedMassIndexingViaJmxTest.class.getSimpleName();
   MBeanServer server;

   @Override
   protected void createCacheManagers() throws Throwable {
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      for (int i = 0; i < NUM_NODES; i++) {
         InputStream is = FileLookupFactory.newInstance().lookupFileStrict(
               "dynamic-indexing-distribution.xml",
               Thread.currentThread().getContextClassLoader());
         ParserRegistry parserRegistry = new ParserRegistry(
               Thread.currentThread().getContextClassLoader());
         ConfigurationBuilderHolder holder = parserRegistry.parse(is);
         // Each cache manager should use a different jmx domain and
         // a parallel-testsuite friendly mbean server
         holder.getGlobalConfigurationBuilder().globalJmxStatistics()
               .jmxDomain(BASE_JMX_DOMAIN + i)
               .mBeanServerLookup(new PerThreadMBeanServerLookup());

         EmbeddedCacheManager cm = TestCacheManagerFactory
               .createClusteredCacheManager(holder, true);
         registerCacheManager(cm);
         Cache cache = cm.getCache();
         caches.add(cache);
      }
      waitForClusterToForm(neededCacheNames);
   }

   @Override
   protected void rebuildIndexes() throws Exception {
      ObjectName massIndexerObjName = getMassIndexerObjectName(
            BASE_JMX_DOMAIN + 0, BasicCacheContainer.DEFAULT_CACHE_NAME);
      server.invoke(massIndexerObjName,
            "start", new Object[]{}, new String[]{});
   }

   ObjectName getMassIndexerObjectName(String jmxDomain, String cacheName) {
      try {
         return new ObjectName(jmxDomain + ":type=Query,name="
               + ObjectName.quote(cacheName) + ",component=MassIndexer");
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Malformed object name", e);
      }
   }

}
