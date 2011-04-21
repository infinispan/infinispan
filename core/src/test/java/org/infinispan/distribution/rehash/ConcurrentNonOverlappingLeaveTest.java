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

import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentNonOverlappingLeaveTest")
public class ConcurrentNonOverlappingLeaveTest extends RehashLeaveTestBase {
   Address l1, l2;

   void performRehashEvent(boolean offline) {
      l1 = addressOf(c2);
      l2 = addressOf(c4);

      CacheContainer cm2 = c2.getCacheManager();
      CacheContainer cm4 = c4.getCacheManager();

      cacheManagers.removeAll(Arrays.asList(cm2, cm4));
      caches.removeAll(Arrays.asList(c2, c4));

      TestingUtil.killCacheManagers(cm2, cm4);
   }
}
