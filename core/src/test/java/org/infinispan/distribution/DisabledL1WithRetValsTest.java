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
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertFalse;

/**
 * Test distribution when L1 is disabled and return values are needed.
 *
 * @author Galder Zamarreño
 * @author Manik Surtani
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

   public void testReplaceFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.replace("k1", "value2");

      assert "value".equals(retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testConditionalReplaceFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      boolean success = nonOwner.replace("k1", "blah", "value2");
      assert !success;

      assertOnAllCachesAndOwnership("k1", "value");

      success = nonOwner.replace("k1", "value", "value2");
      assert success;

      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testPutFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.put("k1", "value2");

      assert "value".equals(retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testRemoveFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.remove("k1");

      assert "value".equals(retval);
      assertRemovedOnAllCaches("k1");
   }

   public void testConditionalRemoveFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      boolean removed = nonOwner.remove("k1", "blah");
      assert !removed;

      assertOnAllCachesAndOwnership("k1", "value");

      removed = nonOwner.remove("k1", "value");
      assert removed;

      assertRemovedOnAllCaches("k1");
   }

   public void testPutIfAbsentFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");

      assert "value".equals(retval);

      assertOnAllCachesAndOwnership("k1", "value");

      c1.clear();

      assertFalse(c1.getAdvancedCache().getLockManager().isLocked("k1"));
      assertFalse(c2.getAdvancedCache().getLockManager().isLocked("k1"));

      retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");
      assert null == retval;

      assertOnAllCachesAndOwnership("k1", "value2");
   }
}
