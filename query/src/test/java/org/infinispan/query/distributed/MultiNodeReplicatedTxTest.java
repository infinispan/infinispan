/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.query.distributed;

import org.infinispan.Cache;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Similiar as MultiNodeReplicatedTest, but uses transactional configuration for the Infinispan.
 *
 * @TODO enable the test when ISPN-2727 is fixed.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeReplicatedTxTest", enabled = false)
public class MultiNodeReplicatedTxTest extends MultiNodeReplicatedTest {

   protected boolean transactionsEnabled() {
      return true;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws IOException {
      ConfigurationBuilderHolder holder = readFromXml();

      holder.getDefaultConfigurationBuilder().transaction().transactionMode(TransactionMode.TRANSACTIONAL);

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(holder, false);
      cacheManagers.add(cacheManager);
      Cache<String, Person> cache = cacheManager.getCache();
      caches.add(cache);
      TestingUtil.waitForRehashToComplete(caches);

      return cacheManager;
   }

   //@TODO enable the test when ISPN-2727 is fixed.
   @Test(enabled = false)
   public void testIndexingWorkDistribution() throws Exception {

   }
}
