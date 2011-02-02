/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Random;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

/**
 * Test distribution when L1 is disabled and return values are needed.
 *
 * @author Galder Zamarreño
 * @since 4.2
 * @since 5.0
 */
@Test(groups = "functional", testName = "distribution.DisabledL1WithRetValsTest")
public class DisabledL1WithRetValsTest extends BaseDistFunctionalTest {

   public DisabledL1WithRetValsTest() {
      l1CacheEnabled = false;
      testRetVals = true;
      numOwners = 1;
      INIT_CLUSTER_SIZE = 2;
   }

   public void testReplaceFromNonOwner(Method m) {
      final String k = k(m);
      final String v = v(m);
      Cache<Object, String> ownerCache = getOwners(k, 1)[0];
      ownerCache.put(k, v);
      Cache<Object, String> nonOwnerCache = getNonOwners(k, 1)[0];
      nonOwnerCache.replace(k, v(m, 1));
   }

}
