/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests verifying the Map Reduce execution for the Topology Aware nodes.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.TopologyAwareTwoNodesMapReduceTest")
public class TopologyAwareTwoNodesMapReduceTest extends SimpleTwoNodesMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder =
            getDefaultClusteredCacheConfig(getCacheMode(), false);

      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfigurationBuilder.transport().machineId("a").rackId("b").siteId("test1");

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(globalConfigurationBuilder,
                                                                                     builder);
      cm1.defineConfiguration(cacheName(), builder.build());
      cacheManagers.add(cm1);

      globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfigurationBuilder.transport().machineId("b").rackId("b").siteId("test2");
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalConfigurationBuilder,
                                                                                     builder);

      cm2.defineConfiguration(cacheName(), builder.build());
      cacheManagers.add(cm2);

      waitForClusterToForm();
   }

   //Overriding these tests with empty body due to additional cache that is created during these tests.
   @Override
   public void testEnsureProperCacheState() throws Exception {

   }

   @Override
   public void testEnsureProperCacheStateMode() {

   }

}