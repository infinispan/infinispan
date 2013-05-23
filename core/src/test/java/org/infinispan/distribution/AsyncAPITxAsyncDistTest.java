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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "distribution.AsyncAPITxAsyncDistTest")
public class AsyncAPITxAsyncDistTest extends AsyncAPITxSyncDistTest {

   private ReplListener rl;
   private ReplListener rlNoTx;

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      rl = new ReplListener(cache(1), true);
      rlNoTx = new ReplListener(cache(1, "noTx"), true);
   }

   @Override
   protected boolean sync() {
      return false;
   }

   @Override
   protected void asyncWait(boolean tx, Class<? extends WriteCommand>... cmds) {
      if (tx) {
         if (cmds == null || cmds.length == 0)
            rl.expectAnyWithTx();
         else
            rl.expectWithTx(cmds);
         rl.waitForRpc(240, TimeUnit.SECONDS);
      } else {
         if (cmds == null || cmds.length == 0)
            rlNoTx.expectAny();
         else
            rlNoTx.expect(cmds);
         rlNoTx.waitForRpc(240, TimeUnit.SECONDS);
      }
   }
}