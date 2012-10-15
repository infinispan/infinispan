/*
 * JBoss, Home of Professional Open Source
 *  Copyright 2012 Red Hat Inc. and/or its affiliates and other
 *  contributors as indicated by the @author tags. All rights reserved
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

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests the following scenario:
 * <pre>
 *  - tx1 originated on N1 writes a single key that maps to N2
 *  - tx1 prepares and before committing crashes
 *  - the prepare is blocked on N2 before reaching the TxInterceptor where the tx is created
 *  - the StaleTransactionCleanupService kicks in on N2 but doesn't clean up the transaction
 *  as it hasn't been prepared yet
 *  - the prepare is now executed on N2
 *  - the test makes sure that the transaction doesn't acquire any locks and doesn't leak
 *  within N1
 *  </pre>
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.PrepareProcessedAfterOriginatorCrashTest")
public class PrepareProcessedAfterOriginatorCrashTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1);
      createClusteredCaches(2, dcc);
   }

   public void testBelatedTransactionDoesntLeak() throws Throwable {

      ComponentRegistry componentRegistry = advancedCache(1).getComponentRegistry();
      final ControlledCommandFactory ccf = new ControlledCommandFactory(componentRegistry.getCommandsFactory(), PrepareCommand.class);
      TestingUtil.replaceField(ccf, "commandsFactory", componentRegistry, ComponentRegistry.class);

      //hack: re-add the component registry to the GlobalComponentRegistry's "namedComponents" (CHM) in order to correctly publish it for
      // when it will be read by the InboundInvocationHandlder. IIH reads the value from the GlobalComponentRegistry.namedComponents before using it
      advancedCache(1).getComponentRegistry().getGlobalComponentRegistry().registerNamedComponentRegistry(componentRegistry, EmbeddedCacheManager.DEFAULT_CACHE_NAME);
      ccf.gate.close();


      Cache receiver = cache(1);
      BlockingPrepareInterceptor interceptor = new BlockingPrepareInterceptor();
      advancedCache(1).addInterceptor(interceptor, 1);

      final Object key = getKeyForCache(1);
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               cache(0).put(key, "v");
            } catch (Throwable e) {
               //possible as the node is being killed
            }
         }
      }, false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return ccf.blockTypeCommandsReceived.get() == 1;
         }
      });

      killMember(0);

      //give the StaleTransactionCleanupService some time to run
      Thread.sleep(5000);

      ccf.gate.open();

      interceptor.prepareExecuted.await();
      log.trace("Finished waiting for belated prepare to complete");

      final TransactionTable transactionTable = TestingUtil.getTransactionTable(receiver);
      assertEquals(0, transactionTable.getRemoteTxCount());
      assertEquals(0, transactionTable.getLocalTxCount());
      assertFalse(receiver.getAdvancedCache().getLockManager().isLocked(key));
   }

   private static class BlockingPrepareInterceptor extends CommandInterceptor {

      public final CountDownLatch prepareExecuted = new CountDownLatch(1);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         try {
            getLog().trace("Processing belated prepare");
            return super.visitPrepareCommand(ctx, command);
         } finally {
            prepareExecuted.countDown();
         }
      }
   }
}
