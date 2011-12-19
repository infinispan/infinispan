/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.lock;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.L1LockTest")
public class L1LockTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.fluent().hash().numOwners(1).transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      createCluster(config, 2);
      waitForClusterToForm();
   }

   public void testConsistency() throws Exception {

      Object localKey = getKeyForCache(0);

      cache(0).put(localKey, "foo");
      assertNotLocked(localKey);

      assertEquals("foo", cache(0).get(localKey));
      assertNotLocked(localKey);

      log.trace("About to perform 2nd get...");
      assertEquals("foo", cache(1).get(localKey));

      assertNotLocked(localKey);

      cache(0).put(localKey, "foo2");
      assertNotLocked(localKey);

      assertEquals("foo2", cache(0).get(localKey));
      assertEquals("foo2", cache(1).get(localKey));


      cache(1).put(localKey, "foo3");
      assertEquals("foo3", cache(0).get(localKey));
      assertEquals("foo3", cache(1).get(localKey));

   }
}
