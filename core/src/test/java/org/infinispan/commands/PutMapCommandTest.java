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

package org.infinispan.commands;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Test(groups = "functional", testName = "commands.PutMapCommandTest")
public class PutMapCommandTest extends MultipleCacheManagersTest {
   protected int numberOfKeys = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1).l1().disable();
      dcc.locking().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }

   public void testPutOnNonOwner() {  //todo [anistor] this does not test putAll !
      MagicKey mk = new MagicKey("key", cache(0));
      cache(3).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).put(mk, "value");

      assert cache(0).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) != null;
      assert cache(1).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) == null;
      assert cache(2).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) == null;
      assert cache(3).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) == null;
   }

   public void testPutMapCommand() {
      for (int i = 0; i < numberOfKeys; ++i) {
         assert cache(0).get("key" + i) == null;
         assert cache(1).get("key" + i) == null;
         assert cache(2).get("key" + i) == null;
         assert cache(3).get("key" + i) == null;
      }

      Map<String, String> map = new HashMap<String, String>();
      for (int i = 0; i < numberOfKeys; ++i) {
         map.put("key" + i, "value" + i);
      }

      cache(0).putAll(map);

      for (int i = 0; i < numberOfKeys; ++i) {
         assertEquals("value" + i, cache(0).get("key" + i));
         final int finalI = i;
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(1).get("key" + finalI).equals("value" + finalI);
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(2).get("key" + finalI).equals("value" + finalI);
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(3).get("key" + finalI).equals("value" + finalI);
            }
         });
      }
   }
}