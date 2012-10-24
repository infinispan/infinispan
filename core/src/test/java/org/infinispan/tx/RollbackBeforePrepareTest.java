/*
 * JBoss, Home of Professional Open Source
 *  Copyright 2012 Red Hat Inc. and/or its affiliates and other
 *  contributors as indicated by the @author tags. All rights reserved.
 *  See the copyright.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as
 *  published by the Free Software Foundation; either version 2.1 of
 *  the License, or (at your option) any later version.
 *
 *  This software is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this software; if not, write to the Free
 *  Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 *  02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.tx;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(testName = "tx.RollbackBeforePrepareTest", groups = "functional")
public class RollbackBeforePrepareTest extends MultipleCacheManagersTest {

   public static final long REPL_TIMEOUT = 1000;
   public static final long LOCK_TIMEOUT = 500;
   private FailPrepareInterceptor failPrepareInterceptor;
   protected CacheMode cacheMode;
   protected int numOwners;

   @Override
   protected void createCacheManagers() throws Throwable {
      cacheMode = CacheMode.REPL_SYNC;
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(cacheMode, true);
      numOwners = 3;
      config
            .locking().lockAcquisitionTimeout(LOCK_TIMEOUT)
            .clustering().sync().replTimeout(REPL_TIMEOUT)
            .clustering().hash().numOwners(numOwners)
            .transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .transaction().completedTxTimeout(3600000);

      createCluster(config, 3);
      waitForClusterToForm();
      failPrepareInterceptor = new FailPrepareInterceptor();
      advancedCache(2).addInterceptor(failPrepareInterceptor, 1);

   }


   public void testCommitNotSentBeforeAllPrepareAreAck() throws Exception {

      ControlledCommandFactory ccf = ControlledCommandFactory.registerControlledCommandFactory(cache(1), PrepareCommand.class);
      ccf.gate.close();

      try {
         cache(0).put("k", "v");
         fail();
      } catch (Exception e) {
         //expected
      }

      //this will also cause a replication timeout
      allowRollbackToRun();

      ccf.gate.open();

      //give some time for the prepare to execute
      Thread.sleep(3000);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int remoteTxCount0 = TestingUtil.getTransactionTable(cache(0)).getRemoteTxCount();
            int remoteTxCount1 = TestingUtil.getTransactionTable(cache(1)).getRemoteTxCount();
            int remoteTxCount2 = TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount();
            log.tracef("remote0=%s, remote1=%s, remote2=%s", remoteTxCount0, remoteTxCount1, remoteTxCount2);
            return remoteTxCount0 == 0 && remoteTxCount1 == 0 && remoteTxCount2 == 0;
         }
      });

      assertNull(cache(0).get("k"));
      assertNull(cache(1).get("k"));
      assertNull(cache(2).get("k"));

      assertNotLocked("k");
   }

   /**
    * by using timeouts here the worse case is to have false positives, i.e. the test to pass when it shouldn't. no
    * false negatives should be possible. In single threaded suit runs this test will generally fail in order
    * to highlight a bug.
    */
   private static void allowRollbackToRun() throws InterruptedException {
      Thread.sleep(REPL_TIMEOUT * 15);
   }

   public static class FailPrepareInterceptor extends CommandInterceptor {

      CountDownLatch failureFinish = new CountDownLatch(1);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         try {
            throw new TimeoutException("Induced!");
         } finally {
            failureFinish.countDown();
         }
      }
   }
}
