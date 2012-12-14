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

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.singlelock.NoPrepareRpcForPessimisticTransactionsTest")
public class NoPrepareRpcForPessimisticTransactionsTest extends MultipleCacheManagersTest {

   private Object k1;
   private ControlledCommandFactory commandFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      final Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      c.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      c.fluent().hash().numOwners(1);
      c.fluent().l1().disable();
      createCluster(c, 2);
      waitForClusterToForm();

      k1 = getKeyForCache(1);
      commandFactory = ControlledCommandFactory.registerControlledCommandFactory(cache(1), CommitCommand.class);
   }

   @BeforeMethod
   void clearStats() {
      commandFactory.remoteCommandsReceived.set(0);
   }

   public void testSingleGetOnPut() throws Exception {

      Operation o = new Operation() {
         @Override
         public void execute() {
            cache(0).put(k1, "v0");
         }
      };

      runtTest(o);
   }

   public void testSingleGetOnRemove() throws Exception {

      Operation o = new Operation() {
         @Override
         public void execute() {
            cache(0).remove(k1);
         }
      };

      runtTest(o);
   }

   private void runtTest(Operation o) throws NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
      log.trace("Here is where the fun starts..");
      tm(0).begin();

      o.execute();

      assertKeyLockedCorrectly(k1);

      assertEquals(commandFactory.remoteCommandsReceived.get(), 2, "2 = cluster get + lock");

      tm(0).commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            //prepare + tx completion notification
            return  commandFactory.remoteCommandsReceived.get()  == 4;
         }
      });
   }

   private interface Operation {
      void execute();
   }
}
