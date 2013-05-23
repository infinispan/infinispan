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
package org.infinispan.invalidation;

import org.infinispan.Cache;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "invalidation.AsyncAPIAsyncInvalTest")
public class AsyncAPIAsyncInvalTest extends AsyncAPISyncInvalTest {

   private ReplListener rl;

   public AsyncAPIAsyncInvalTest() {
      cleanup = AbstractCacheTest.CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      Cache c2 = cache(1, super.getClass().getSimpleName());
      rl = new ReplListener(c2, true);
   }

   @Override
   protected boolean sync() {
      return false;
   }
   
   @Override
   protected void resetListeners() {
      rl.resetEager();
   }

   @Override
   protected void asyncWait(Class<? extends WriteCommand>... cmds) {
      if (cmds == null || cmds.length == 0)
         rl.expectAny();
      else
         rl.expect(cmds);

      rl.waitForRpc();
   }
}
