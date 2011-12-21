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
package org.infinispan.cacheviews;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.Util;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "cacheviews.CacheViewsManagerRecoverViewsTest")
@CleanupAfterMethod
public class CacheViewsManagerRecoverViewsTest extends MultipleCacheManagersTest {

   Cache c1, c2;
   DISCARD d1, d2;

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, "cache",
            getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC),
                  new TransportFlags().withFD(true).withMerge(true));

      c1 = cache(0, "cache");
      c2 = cache(1, "cache");
      d1 = TestingUtil.getDiscardForCache(c1);
      d1.setExcludeItself(true);
      d2 = TestingUtil.getDiscardForCache(c2);
      d2.setExcludeItself(true);
   }

   public void testRecoverLoop() throws Exception {
      for (int i = 0; i < 3; i++) {
         // create the partitions
         log.debugf("Splitting the cluster in two");
         d1.setDiscardAll(true);
         d2.setDiscardAll(true);

         // wait for the partitions to form
         TestingUtil.blockUntilViewsReceived(60000, false, c1);
         TestingUtil.waitForRehashToComplete(c1);
         TestingUtil.blockUntilViewsReceived(60000, false, c2);
         TestingUtil.waitForRehashToComplete(c2);

         // merge the two partitions
         log.debugf("Merging the cluster partitions");
         d1.setDiscardAll(false);
         d2.setDiscardAll(false);
         // wait for the merged cluster to form
         long startTime = System.currentTimeMillis();
         TestingUtil.blockUntilViewsReceived(30000, c1, c2);
         TestingUtil.waitForRehashToComplete(c1, c2);
         long endTime = System.currentTimeMillis();

         log.debugf("Merge took %s", Util.prettyPrintTime(endTime - startTime));
         assert endTime - startTime < 30000 : "Merge took too long: " + Util.prettyPrintTime(endTime - startTime);
      }
      log.debugf("Test finished without any failure");
   }
}
