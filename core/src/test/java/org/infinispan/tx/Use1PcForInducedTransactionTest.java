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

package org.infinispan.tx;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test (groups = "functional", testName = "tx.Use1PcForInducedTransactionTest")
public class Use1PcForInducedTransactionTest extends MultipleCacheManagersTest {

   private InvocationCountInterceptor ic0;
   private InvocationCountInterceptor ic1;

   @Override
   protected void createCacheManagers() throws Throwable {

      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      c.fluent().transaction().use1PcForAutoCommitTransactions(true);
      assert c.isUse1PcForAutoCommitTransactions();

      createCluster(c, 2);
      waitForClusterToForm();

      ic0 = new InvocationCountInterceptor();
      advancedCache(0).addInterceptor(ic0, 1);
      ic1 = new InvocationCountInterceptor();
      advancedCache(1).addInterceptor(ic1, 1);
   }

   public void testSinglePhaseCommit() {
      cache(0).put("k", "v");
      assert cache(0).get("k").equals("v");
      assert cache(1).get("k").equals("v");

      assertNotLocked("k");

      assertEquals(ic0.prepareInvocations, 1);
      assertEquals(ic1.prepareInvocations, 1);
      assertEquals(ic0.commitInvocations, 0);
      assertEquals(ic0.commitInvocations, 0);
   }


   public static class InvocationCountInterceptor extends CommandInterceptor {

      volatile int prepareInvocations;
      volatile int commitInvocations;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         prepareInvocations ++;
         return super.visitPrepareCommand(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitInvocations ++;
         return super.visitCommitCommand(ctx, command);
      }
   }
}
