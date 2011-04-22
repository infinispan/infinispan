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
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "distribution.DistAsyncTxFuncTest")
public class DistAsyncTxFuncTest extends DistSyncTxFuncTest {

   ReplListener r1, r2, r3, r4;
   ReplListener[] r;
   Map<Cache<?, ?>, ReplListener> listenerLookup;
   List<Address> listenerCaches;

   public DistAsyncTxFuncTest() {
      sync = false;
      tx = true;
      testRetVals = true;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      r1 = new ReplListener(c1, true, true);
      r2 = new ReplListener(c2, true, true);
      r3 = new ReplListener(c3, true, true);
      r4 = new ReplListener(c4, true, true);
      r = new ReplListener[]{r1, r2, r3, r4};
      listenerLookup = new HashMap<Cache<?, ?>, ReplListener>();
      listenerCaches = new ArrayList<Address>();
      for (ReplListener rl : r) {
         listenerCaches.add(addressOf(rl.getCache()));
         listenerLookup.put(rl.getCache(), rl);
      }
   }
  
   @Override
   protected void asyncWait(Object key, Class<? extends VisitableCommand> command, Cache<?, ?>... cachesOnWhichKeyShouldInval) {
      if (cachesOnWhichKeyShouldInval == null) cachesOnWhichKeyShouldInval = new Cache[0];
      List<Cache<?, ?>> cachesOnWhichKeyShouldInvalList = new ArrayList(Arrays.asList(cachesOnWhichKeyShouldInval));
      if (key == null) {
         // test all caches.
         for (ReplListener rl : r) rl.expect(command);
         for (ReplListener rl : r) rl.waitForRpc();
      } else {
         for (Cache<?, ?> c : getOwners(key)) {
            log.info("Analysing cache " + addressOf(c) + ".  Listeners are avbl for caches " + listenerCaches);
            if (cachesOnWhichKeyShouldInvalList.remove(c)) {
               listenerLookup.get(c).expect(command, InvalidateL1Command.class);
            } else {
               listenerLookup.get(c).expect(command);
            }
            listenerLookup.get(c).waitForRpc();
         }

         for (Cache<?, ?> c : cachesOnWhichKeyShouldInvalList) {
            listenerLookup.get(c).expect(InvalidateL1Command.class);
            listenerLookup.get(c).waitForRpc();
         }
      }
   }

   @Override
   protected void asyncTxWait(Object... keys) {
      // Wait for a tx completion event
      if (keys != null) {

         Set<Cache<?, ?>> cachesInTx = new HashSet<Cache<?, ?>>();
         for (Object k : keys) {
            cachesInTx.addAll(Arrays.asList(getOwners(k)));
         }

         log.warn("In asyncTxWait, waiting for repl events on caches " + cachesInTx + " on keys " + Arrays.toString(keys));

         for (Cache<?, ?> c : cachesInTx) {
            listenerLookup.get(c).expectAnyWithTx();
            listenerLookup.get(c).waitForRpc(240, TimeUnit.SECONDS);
         }

         // This sucks but for async transactions we still need this!!
         TestingUtil.sleepThread(500 * keys.length);
      }
   }
}