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

package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.replication.AsyncAPINonTxSyncReplTest;
import org.infinispan.test.data.Key;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;

@Test(groups = "functional", testName = "distribution.AsyncAPINonTxSyncDistTest")
public class AsyncAPINonTxSyncDistTest extends AsyncAPINonTxSyncReplTest {

   @Override
   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(sync() ? CacheMode.DIST_SYNC : CacheMode.DIST_ASYNC, false);
   }

   @Override
   protected void assertOnAllCaches(final Key k, final String v, final Cache c1, final Cache c2) {
      if (!sync()) {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return Util.safeEquals(c1.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k), v) &&
                     Util.safeEquals(c2.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k), v);
            }
         });
      } else {
         Object real;
         assert Util.safeEquals((real = c1.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
         assert Util.safeEquals((real = c2.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
      }
   }

   @Test(enabled = false, description = "Disabled due to https://issues.jboss.org/browse/ISPN-3133")
   @Override
   public void testAsyncMethods() throws Exception {
      super.testAsyncMethods();
   }
}
