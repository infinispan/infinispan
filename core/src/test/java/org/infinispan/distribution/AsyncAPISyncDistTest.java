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
package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import org.infinispan.replication.AsyncAPISyncReplTest;
import org.infinispan.test.data.Key;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "distribution.AsyncAPISyncDistTest")
public class AsyncAPISyncDistTest extends AsyncAPISyncReplTest {

   @SuppressWarnings("unchecked")
   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c =
            getDefaultClusteredConfig(sync() ? Configuration.CacheMode.DIST_SYNC : Configuration.CacheMode.DIST_ASYNC, true);
      c.setLockAcquisitionTimeout(30, TimeUnit.SECONDS);
      List<Cache<Key, String>> l = createClusteredCaches(2, getClass().getSimpleName(), c);

      // wait for any rehashing to complete
      waitForClusterToForm();
   }

   @Override
   protected void assertOnAllCaches(Key k, String v, Cache c1, Cache c2) {
      Object real;
      assert Util.safeEquals((real = c1.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
      assert Util.safeEquals((real = c2.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
   }
}