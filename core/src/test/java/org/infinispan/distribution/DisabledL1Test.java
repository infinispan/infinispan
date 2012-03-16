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
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

@Test(groups = "functional", testName = "distribution.DisabledL1Test")
public class DisabledL1Test extends BaseDistFunctionalTest {

   public DisabledL1Test () {
      sync = true;
      tx = false;
      testRetVals = false;
      l1CacheEnabled = false;
   }
   
   public void testRemoveFromNonOwner() {
      for (Cache<Object, String> c : caches) assert c.isEmpty();
      
      Object retval = getFirstNonOwner("k1").put("k1", "value");
      asyncWait("k1", PutKeyValueCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assert retval == null;
      
      retval = getOwners("k1")[0].remove("k1");
      asyncWait("k1", RemoveCommand.class, getFirstNonOwner("k1"));
      if (testRetVals) assert "value".equals(retval);

      assertRemovedOnAllCaches("k1");
   }

   public void testReplaceFromNonOwner(Method m) {
      final String k = k(m);
      final String v = v(m);
      getOwners(k)[0].put(k, v);
      getNonOwners(k)[0].replace(k, v(m, 1));
   }

}
