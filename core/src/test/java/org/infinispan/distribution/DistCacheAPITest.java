/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution;

import org.infinispan.Cache;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "distribution.DistCacheAPITest")
public class DistCacheAPITest extends BaseDistFunctionalTest {
   
   public DistCacheAPITest() {
      sync = true;
      tx = false;
      l1CacheEnabled = false;
   }
   
   public void testPutIfAbsent() {
      final String firstValue = this.getClass().getName() + "putIfAbsentValue1";
      final String secondValue = this.getClass().getName() + "putIfAbsentValue2";
      
      assertAllCachesState("k1", null);
      
      Object retval = getFirstNonOwner("k1").putIfAbsent("k1", firstValue);
      assert retval == null;
      assertAllCachesState("k1", firstValue);
      
      assert getFirstNonOwner("k1").containsKey("k1");
      retval = getFirstNonOwner("k1").putIfAbsent("k1", secondValue);
      assert retval != null;
      assert retval.equals(firstValue);
      
      assertAllCachesState("k1", firstValue);
   }

   private void assertAllCachesState(String key, Object expectedValue) {
      for (Cache cache : caches) {
         Object value = cache.get(key);
         if ( expectedValue == null ) {
            assert value == null;
         }
         else {
            expectedValue.equals(value);
         }
      }
   }

}
