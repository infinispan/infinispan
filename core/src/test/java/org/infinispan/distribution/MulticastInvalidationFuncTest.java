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
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.test.ReplListener;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;

@Test(groups = "functional", testName = "distribution.MulticastInvalidationFuncTest")
public class MulticastInvalidationFuncTest extends BaseDistFunctionalTest {
	
	public static final String KEY1 = "k1";

   public MulticastInvalidationFuncTest() {
      sync = true;
      tx = false;
      testRetVals = true;
      l1Threshold = 0;
   }

   public void testPut() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner(KEY1);
      Cache<Object, String> owner = getOwners(KEY1)[0];
      Collection<ReplListener> listeners = new ArrayList<ReplListener>();
      
      // Put an object in from a non-owner, this will cause an L1 record to be created there
      
      nonOwner.put(KEY1, "foo");
      Assert.assertEquals(nonOwner.getAdvancedCache().getDataContainer().get(KEY1).getValue(), "foo");
      Assert.assertEquals(owner.getAdvancedCache().getDataContainer().get(KEY1).getValue(), "foo");
      
      // Check that all nodes (except the one we put to) are notified
      // but only if the transport is multicast-capable
      if (owner.getAdvancedCache().getRpcManager().getTransport().isMulticastCapable()) {
         for (Cache<Object, String> c : getNonOwners(KEY1)) {
            ReplListener rl = new ReplListener(c);
            rl.expect(InvalidateL1Command.class);
            listeners.add(rl);
            log.debugf("Added nonowner %s", c);
         }
      } else {
         ReplListener rl = new ReplListener(nonOwner);
         rl.expect(InvalidateL1Command.class);
         listeners.add(rl);
      }
      
      // Put an object into an owner, this will cause the L1 records for this key to be invalidated
      owner.put(KEY1, "bar");
      
      for (ReplListener rl : listeners) {
      	rl.waitForRpc();
      }
      
      Assert.assertNull(nonOwner.getAdvancedCache().getDataContainer().get(KEY1));
   }
   
}
