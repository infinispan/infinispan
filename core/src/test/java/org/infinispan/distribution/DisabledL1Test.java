/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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

      assertOnAllCachesAndOwnership("k1", null);
   }

}
