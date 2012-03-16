/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tx.NoAutoCommitAndPferTest")
public class NoAutoCommitAndPferTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration dsc = getDefaultStandaloneConfig(true);
      dsc.fluent().transaction().autoCommit(false);
      return new DefaultCacheManager(dsc);
   }

   public void testPferNoAutoCommitExplicitTransaction() throws Exception {
      tm().begin();
      cache.putForExternalRead("k1","v");
      tm().commit();
      assert cache.get("k1").equals("v"); //here is the failure!
   }

   public void testPferNoAutoCommit() throws Exception {
      cache.putForExternalRead("k2","v");
      assert cache.get("k2").equals("v"); //here is the failure!
   }

}