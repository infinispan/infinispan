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

package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests if clear operation actually succeeds in removing all keys from all nodes of a distributed cluster.
 * See https://issues.jboss.org/browse/ISPN-2530.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional")
public abstract class BaseDistClearTest extends MultipleCacheManagersTest {

   protected final Log log = LogFactory.getLog(getClass());

   protected AdvancedCache<Integer, String> c0;
   protected AdvancedCache<Integer, String> c1;
   protected AdvancedCache<Integer, String> c2;

   private final ConfigurationBuilder builder;

   protected BaseDistClearTest(boolean transactional, boolean optimistic) {
      builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, transactional, transactional);
      builder.clustering().hash().numSegments(3).numOwners(2)
            .stateTransfer().fetchInMemoryState(true)
            .locking().lockAcquisitionTimeout(1000l);

      if (transactional) {
         builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
               .transactionManagerLookup(new DummyTransactionManagerLookup())
               .syncCommitPhase(true).syncRollbackPhase(true)
               .lockingMode(optimistic ? LockingMode.OPTIMISTIC : LockingMode.PESSIMISTIC);
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(builder, 3);
      waitForClusterToForm();

      c0 = advancedCache(0);
      c1 = advancedCache(1);
      c2 = advancedCache(2);
   }

   public void testClear() throws Exception {
      final int numKeys = 5;
      log.infof("Putting %d keys into cache ..", numKeys);
      for (int i = 0; i < numKeys; i++) {
         String value = "val_" + i;
         c0.put(i, value);

         // force all values into L1 of the other nodes
         assertEquals(value, c0.get(i));
         assertEquals(value, c1.get(i));
         assertEquals(value, c2.get(i));
      }
      log.info("Finished putting keys");

      DataContainer dc0 = c0.getDataContainer();
      DataContainer dc1 = c1.getDataContainer();
      DataContainer dc2 = c2.getDataContainer();

      assertTrue(dc0.size() > 0);
      assertTrue(dc1.size() > 0);
      assertTrue(dc2.size() > 0);

      log.info("Clearing cache ..");
      c0.clear();
      log.info("Finished clearing cache");

      assertEquals(0, dc0.size());
      assertEquals(0, dc1.size());
      assertEquals(0, dc2.size());
   }
}
