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
package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups = "functional", testName = "distribution.rehash.SingleJoinTest")
public class SingleJoinTest extends RehashTestBase {
   EmbeddedCacheManager joinerManager;
   Cache<Object, String> joiner;

   void performRehashEvent(boolean offline) {
      joinerManager = addClusterEnabledCacheManager(new TransportFlags().withFD(true));
      joinerManager.defineConfiguration(cacheName, configuration);
      joiner = joinerManager.getCache(cacheName);
   }

   void waitForRehashCompletion() {
      // need to block until this join has completed!
      List<Cache> allCaches = new ArrayList(caches);
      allCaches.add(joiner);
      TestingUtil.blockUntilViewsReceived(60000, allCaches);
      waitForClusterToForm(cacheName);

      // where does the joiner sit in relation to the other caches?
      int joinerPos = locateJoiner(joinerManager.getAddress());

      log.info("***>>> Joiner is in position " + joinerPos);

      cacheManagers.add(joinerPos, joinerManager);
      caches.add(joinerPos, joiner);
   }
}
