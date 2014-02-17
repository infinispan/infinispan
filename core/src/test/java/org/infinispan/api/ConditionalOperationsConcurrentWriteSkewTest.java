package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.ConditionalOperationsConcurrentWriteSkewTest")
public class ConditionalOperationsConcurrentWriteSkewTest extends ConditionalOperationsConcurrentTest {

   public ConditionalOperationsConcurrentWriteSkewTest() {
      transactional = true;
      writeSkewCheck = true;
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

   @Override
   public void testReplace() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ReplaceOperation(true));
   }

   @Override
   public void testConditionalRemove() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ConditionalRemoveOperation(true));
   }

   @Override
   public void testPutIfAbsent() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new PutIfAbsentOperation(true));
   }

   /**
    * This is a specific scenario of the {@link #testOnCaches(java.util.List, org.infinispan.api.ConditionalOperationsConcurrentTest.CacheOperation)}
    * test
    */
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

         controller.awaitCommit = new CountDownLatch(1);
         controller.blockCommit = new CountDownLatch(1);

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
         controller.blockRemoteGet = new CountDownLatch(1);

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

      private volatile CountDownLatch blockRemoteGet = null;
      private volatile CountDownLatch blockCommit = null;
      private volatile CountDownLatch awaitPrepare = null;
      private volatile CountDownLatch awaitCommit = null;

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            getLog().debug("visit Get");
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
               awaitPrepare.countDown();
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
                  awaitCommit.countDown();
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
            blockCommit.countDown();
            blockCommit = null;
         }
         if (blockRemoteGet != null) {
            blockRemoteGet.countDown();
            blockRemoteGet = null;
         }
         if (awaitPrepare != null) {
            awaitPrepare.countDown();
            awaitPrepare = null;
         }
         if (awaitCommit != null) {
            awaitCommit.countDown();
            awaitCommit = null;
         }
      }
   }
}
