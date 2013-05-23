/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "functional", testName = "tx.OnePhaseXATest", description = "See ISPN-156 for details.")
public class OnePhaseXATest extends AbstractInfinispanTest {
   private List<Cache> caches;
   private List<EmbeddedCacheManager> cacheContainers;
   public static final int CACHES_NUM = 2;

   public void testMultipleCaches() throws Exception {

      //add something  to cache
      int i = 0;
      for (Cache c : caches) {
         TransactionManager tm = TestingUtil.getTransactionManager(c);
         tm.begin();
         c.put("key" + i, "value");
         tm.commit();
         i++;
      }

      //check if caches contain these same keys
      i = 0;
      for (Cache c : caches) {
         assert "value".equals(c.get("key0")) : "Failed getting value for key0 on cache " + i;
         assert "value".equals(c.get("key1")) : "Failed getting value for key1 on cache " + i;
         i++;
      }
   }

   @BeforeTest
   public void setUp() throws Exception {
      caches = new ArrayList<Cache>();
      cacheContainers = new ArrayList<EmbeddedCacheManager>();
      for (int i = 0; i < CACHES_NUM; i++) caches.add(getCache());
   }

   @AfterTest
   public void tearDown() {
      if (caches != null) TestingUtil.killCacheManagers(cacheContainers);
   }

   private Cache getCache() {
      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();

      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.REPL_SYNC)
            .sync().replTimeout(30000)
            .transaction().invocationBatching().enable()
            .transaction().syncCommitPhase(true)
            .locking().lockAcquisitionTimeout(60000).useLockStriping(false);
      EmbeddedCacheManager container = TestCacheManagerFactory.createCacheManager(gc, c);
      cacheContainers.add(container);
      return container.getCache("TestCache");
   }
}