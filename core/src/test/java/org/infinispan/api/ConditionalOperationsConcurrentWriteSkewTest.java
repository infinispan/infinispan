package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.ConditionalOperationsConcurrentWriteSkewTest")
public class ConditionalOperationsConcurrentWriteSkewTest extends MultipleCacheManagersTest {

   private static final int NODES_NUM = 3;

   private final CacheMode mode = CacheMode.DIST_SYNC;
   protected LockingMode lockingMode = LockingMode.OPTIMISTIC;
   protected boolean writeSkewCheck;
   protected boolean transactional;

   public ConditionalOperationsConcurrentWriteSkewTest() {
      transactional = true;
      writeSkewCheck = true;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(mode, true);
      dcc.transaction().lockingMode(lockingMode);
      if (writeSkewCheck) {
         dcc.transaction().locking().writeSkewCheck(true);
         dcc.transaction().locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
         dcc.transaction().versioning().enable().scheme(VersioningScheme.SIMPLE);
      }
      createCluster(dcc, NODES_NUM);
      waitForClusterToForm();
   }

   public void testSimpleConcurrentReplace() throws Exception {
      doSimpleConcurrentTest(Operation.REPLACE);
   }

   public void testSimpleConcurrentPut() throws Exception {
      doSimpleConcurrentTest(Operation.PUT);
   }

   public void testSimpleConcurrentRemove() throws Exception {
      doSimpleConcurrentTest(Operation.REMOVE);
   }

   private void doSimpleConcurrentTest(final Operation operation) throws Exception {
      //default owners are 2
      assertEquals("Wrong number of owner. Please change the configuration", 2,
                   cache(0).getCacheConfiguration().clustering().hash().numOwners());
      final Object key = new MagicKey(cache(0), cache(1));

      try {
         CommandInterceptorController controller = injectController(cache(1));

         if (operation == Operation.REMOVE || operation == Operation.REPLACE) {
            cache(0).put(key, "v1");
         }

         controller.awaitCommit.close();
         controller.blockCommit.close();

         final Future<Boolean> tx1 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(1).begin();
               cache(1).put(key, "tx1");
               tm(1).commit();
               return Boolean.TRUE;
            }
         });

         //await tx1 commit on cache1... the commit will be blocked!
         //tx1 has already committed in cache(0) but not in cache(1)
         //we block the remote get in order to force the tx2 to read the most recent value from cache(0)
         controller.awaitCommit.await();
         controller.blockRemoteGet.close();

         final Future<Boolean> tx2 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(2).begin();
               switch (operation) {
                  case REMOVE:
                     cache(2).remove(key, "v1");
                     break;
                  case REPLACE:
                     cache(2).replace(key, "v1", "tx2");
                     break;
                  case PUT:
                     cache(2).putIfAbsent(key, "tx2");
                     break;
               }
               tm(2).commit();
               return Boolean.TRUE;
            }
         });

         //tx2 will not prepare the transaction remotely since the operation should fail.
         assertTrue("Tx2 has not finished", tx2.get(20, TimeUnit.SECONDS));

         //let everything run normally
         controller.reset();

         assertTrue("Tx1 has not finished", tx1.get(20, TimeUnit.SECONDS));


         //check if no transactions are active
         assertNoTransactions();

         for (Cache cache : caches()) {
            assertEquals("Wrong value for cache " + address(cache), "tx1", cache.get(key));
         }
      } finally {
         removeController(cache(1));
      }

   }

   private CommandInterceptorController injectController(Cache cache) {
      CommandInterceptorController commandInterceptorController = new CommandInterceptorController();
      cache.getAdvancedCache().addInterceptorBefore(commandInterceptorController, VersionedDistributionInterceptor.class);
      return commandInterceptorController;
   }

   private void removeController(Cache cache) {
      cache.getAdvancedCache().removeInterceptor(CommandInterceptorController.class);
   }

   private enum Operation {
      PUT, REPLACE, REMOVE
   }

   private class CommandInterceptorController extends BaseCustomInterceptor {

      private final ReclosableLatch blockRemoteGet = new ReclosableLatch(true);
      private final ReclosableLatch blockCommit = new ReclosableLatch(true);
      private final ReclosableLatch awaitPrepare = new ReclosableLatch(true);
      private final ReclosableLatch awaitCommit = new ReclosableLatch(true);

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            getLog().debug("visit GetKeyValueCommand");
            if (!ctx.isOriginLocal() && blockRemoteGet != null) {
               getLog().debug("Remote Get Received... blocking...");
               blockRemoteGet.await();
            }
         }
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            getLog().debug("visit GetCacheEntryCommand");
            if (!ctx.isOriginLocal() && blockRemoteGet != null) {
               getLog().debug("Remote Get Received... blocking...");
               blockRemoteGet.await();
            }
         }
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            getLog().debug("visit Prepare");
            if (awaitPrepare != null) {
               getLog().debug("Prepare Received... unblocking");
               awaitPrepare.open();
            }
         }
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            if (ctx.isOriginLocal()) {
               getLog().debug("visit Commit");
               if (awaitCommit != null) {
                  getLog().debug("Commit Received... unblocking...");
                  awaitCommit.open();
               }
               if (blockCommit != null) {
                  getLog().debug("Commit Received... blocking...");
                  blockCommit.await();
               }
            }
         }
      }

      public void reset() {
         if (blockCommit != null) {
            blockCommit.open();
         }
         if (blockRemoteGet != null) {
            blockRemoteGet.open();
         }
         if (awaitPrepare != null) {
            awaitPrepare.open();
         }
         if (awaitCommit != null) {
            awaitCommit.open();
         }
      }
   }
}
