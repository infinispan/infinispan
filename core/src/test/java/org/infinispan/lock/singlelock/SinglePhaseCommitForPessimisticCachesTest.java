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

package org.infinispan.lock.singlelock;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Single phase commit is used with pessimistic caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.SinglePhaseCommitForPessimisticCachesTest")
public class SinglePhaseCommitForPessimisticCachesTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      final Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      c.fluent().hash().numOwners(3);
      c.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(c, 3);
      waitForClusterToForm();
   }

   public void testSinglePhaseCommit() throws Exception {
      final Object k0_1 = getKeyForCache(0);
      final Object k0_2 = getKeyForCache(0);

      final List<Address> members = advancedCache(0).getRpcManager().getTransport().getMembers();
      assert advancedCache(0).getDistributionManager().locate(k0_1).containsAll(members);
      assert advancedCache(0).getDistributionManager().locate(k0_2).containsAll(members);
      TxCountInterceptor interceptor0 = new TxCountInterceptor();
      TxCountInterceptor interceptor1 = new TxCountInterceptor();
      advancedCache(0).addInterceptor(interceptor0, 1);
      advancedCache(1).addInterceptor(interceptor1, 2);

      tm(2).begin();
      cache(2).put(k0_1, "v");
      cache(2).put(k0_2, "v");
      tm(2).commit();


      assertEquals(interceptor0.lockCount, 2);
      assertEquals(interceptor1.lockCount, 2);
      assertEquals(interceptor0.prepareCount, 1);
      assertEquals(interceptor1.prepareCount, 1);
      assertEquals(interceptor0.commitCount, 0);
      assertEquals(interceptor1.commitCount, 0);
   }


   public static class TxCountInterceptor extends CommandInterceptor {

      public volatile int prepareCount;
      public volatile int commitCount;
      public volatile int lockCount;
      public volatile int putCount;
      public volatile int getCount;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         prepareCount++;
         return super.visitPrepareCommand(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitCount++;
         return super.visitCommitCommand(ctx, command);
      }

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         lockCount++;
         return super.visitLockControlCommand(ctx, command);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         putCount++;
         return super.visitPutKeyValueCommand(ctx, command);
      }

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         getCount++;
         return super.visitGetKeyValueCommand(ctx, command);
      }
   }

}
