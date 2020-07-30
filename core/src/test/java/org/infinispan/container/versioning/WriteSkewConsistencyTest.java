package org.infinispan.container.versioning;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.infinispan.transaction.impl.WriteSkewHelper.versionFromEntry;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "container.versioning.WriteSkewConsistencyTest")
@CleanupAfterMethod
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC})
public class WriteSkewConsistencyTest extends MultipleCacheManagersTest {

   public void testValidationOnlyInPrimaryOwner() throws Exception {
      // ControlledConsistentHashFactory sets cache(1) as the primary owner and cache(0) as the backup for all keys
      final Object key = "key";
      final DataContainer<?, ?> primaryOwnerDataContainer = extractComponent(cache(1), InternalDataContainer.class);
      final DataContainer<?, ?> backupOwnerDataContainer = extractComponent(cache(0), InternalDataContainer.class);
      final VersionGenerator versionGenerator = extractComponent(cache(1), VersionGenerator.class);

      injectReorderResponseRpcManager(cache(3), cache(0));
      cache(1).put(key, 1);
      for (Cache<?, ?> cache : caches()) {
         assertEquals("Wrong initial value for cache " + address(cache), 1, cache.get(key));
      }

      InternalCacheEntry<? ,?> ice0 = primaryOwnerDataContainer.peek(key);
      InternalCacheEntry<?, ?> ice1 = backupOwnerDataContainer.peek(key);
      assertSameVersion("Wrong version for the same key", versionFromEntry(ice0), versionFromEntry(ice1));

      final IncrementableEntryVersion version0 = versionFromEntry(ice0);
      //version1 is put by tx1
      final IncrementableEntryVersion version1 = versionGenerator.increment(version0);
      //version2 is put by tx2
      final IncrementableEntryVersion version2 = versionGenerator.increment(version1);

      ControllerInboundInvocationHandler handler = wrapInboundInvocationHandler(cache(0), ControllerInboundInvocationHandler::new);
      BackupOwnerInterceptor backupOwnerInterceptor = injectBackupOwnerInterceptor(cache(0));
      backupOwnerInterceptor.blockCommit(true);
      handler.discardRemoteGet = true;

      Future<Boolean> tx1 = fork(() -> {
         tm(2).begin();
         assertEquals("Wrong value for tx1.", 1, cache(2).get(key));
         cache(2).put(key, 2);
         tm(2).commit();
         return Boolean.TRUE;
      });

      //after this, the primary owner has committed the new value but still have the locks acquired.
      //in the backup owner, it still has the old value

      eventually(() -> {
         Integer value = (Integer) cache(3).get(key);
         return value != null && value == 2;
      });

      assertSameVersion("Wrong version in the primary owner", versionFromEntry(primaryOwnerDataContainer.peek(key)),
                    version1);
      assertSameVersion("Wrong version in the backup owner", versionFromEntry(backupOwnerDataContainer.peek(key)),
                    version0);

      backupOwnerInterceptor.resetPrepare();

      //tx2 will read from the primary owner (i.e., the most recent value) and will commit.
      Future<Boolean> tx2 = fork(() -> {
         tm(3).begin();
         assertEquals("Wrong value for tx2.", 2, cache(3).get(key));
         cache(3).put(key, 3);
         tm(3).commit();
         return Boolean.TRUE;
      });

      //if everything works fine, it should ignore the value from the backup owner and only use the version returned by
      //the primary owner.
      AssertJUnit.assertTrue("Prepare of tx2 was never received.", backupOwnerInterceptor.awaitPrepare(10000));

      backupOwnerInterceptor.blockCommit(false);
      handler.discardRemoteGet = false;

      //both transaction should commit
      AssertJUnit.assertTrue("Error in tx1.", tx1.get(15, SECONDS));
      AssertJUnit.assertTrue("Error in tx2.", tx2.get(15, SECONDS));

      //both txs has committed
      assertSameVersion("Wrong version in the primary owner", versionFromEntry(primaryOwnerDataContainer.peek(key)),
                    version2);
      assertSameVersion("Wrong version in the backup owner", versionFromEntry(backupOwnerDataContainer.peek(key)),
                    version2);


      assertNoTransactions();
      assertNotLocked(key);
   }

   @Override
   protected final void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      ControlledConsistentHashFactory consistentHashFactory =
            cacheMode.isReplicated() ?
            new ControlledConsistentHashFactory.Replicated(1) :
            new ControlledConsistentHashFactory.Default(1, 0);
      builder.clustering().hash().numSegments(1).consistentHashFactory(consistentHashFactory);
      createClusteredCaches(4, TestDataSCI.INSTANCE, builder);
   }

   private BackupOwnerInterceptor injectBackupOwnerInterceptor(Cache<?, ?> cache) {
      BackupOwnerInterceptor ownerInterceptor = new BackupOwnerInterceptor();
      extractInterceptorChain(cache).addInterceptor(ownerInterceptor, 1);
      return ownerInterceptor;
   }

   private void injectReorderResponseRpcManager(Cache<?, ?> toInject, Cache<?, ?> lastResponse) {
      RpcManager rpcManager = extractComponent(toInject, RpcManager.class);
      ReorderResponsesRpcManager newRpcManager = new ReorderResponsesRpcManager(address(lastResponse), rpcManager);
      replaceComponent(toInject, RpcManager.class, newRpcManager, true);
   }

   private void assertSameVersion(String message, EntryVersion v0, EntryVersion v1) {
      assertEquals(message, InequalVersionComparisonResult.EQUAL, v0.compareTo(v1));
   }

   @SuppressWarnings("rawtypes")
   static class BackupOwnerInterceptor extends DDAsyncInterceptor {

      private final Object blockCommitLock = new Object();
      private final Object prepareProcessedLock = new Object();
      private boolean blockCommit;
      private boolean prepareProcessed;

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         blockIfNeeded();
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> notifyPrepareProcessed());
      }

      public void blockCommit(boolean blockCommit) {
         synchronized (blockCommitLock) {
            this.blockCommit = blockCommit;
            if (!blockCommit) {
               blockCommitLock.notifyAll();
            }
         }
      }

      public boolean awaitPrepare(long milliseconds) throws InterruptedException {
         synchronized (prepareProcessedLock) {
            long endTime = System.nanoTime() + MILLISECONDS.toNanos(milliseconds);
            long sleepTime = NANOSECONDS.toMillis(endTime - System.nanoTime());
            while (!prepareProcessed && sleepTime > 0) {
               prepareProcessedLock.wait(sleepTime);
               sleepTime = NANOSECONDS.toMillis(endTime - System.nanoTime());
            }
            return prepareProcessed;
         }
      }

      public void resetPrepare() {
         synchronized (prepareProcessedLock) {
            prepareProcessed = false;
         }
      }

      private void notifyPrepareProcessed() {
         synchronized (prepareProcessedLock) {
            prepareProcessed = true;
            prepareProcessedLock.notifyAll();
         }
      }

      private void blockIfNeeded() throws InterruptedException {
         synchronized (blockCommitLock) {
            while (blockCommit) {
               blockCommitLock.wait();
            }
         }
      }
   }

   private class ReorderResponsesRpcManager extends AbstractDelegatingRpcManager {

      private final Address lastResponse;

      public ReorderResponsesRpcManager(Address lastResponse, RpcManager realOne) {
         super(realOne);
         this.lastResponse = lastResponse;
      }

      @SuppressWarnings("unchecked")
      @Override
      protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                      ResponseCollector<T> collector,
                                                      Function<ResponseCollector<T>, CompletionStage<T>> invoker,
                                                      RpcOptions rpcOptions) {
         return super.performRequest(targets, command, collector, invoker, rpcOptions)
                     .thenApply(responseObject -> {
                        if (!(responseObject instanceof Map)) {
                           log.debugf("Single response for command %s: %s", command, responseObject);
                           return responseObject;
                        }

                        Map<Address, Response> newResponseMap = new LinkedHashMap<>();
                        boolean containsLastResponseAddress = false;
                        for (Map.Entry<Address, Response> entry : ((Map<Address, Response>) responseObject)
                                                                     .entrySet()) {
                           if (lastResponse.equals(entry.getKey())) {
                              containsLastResponseAddress = true;
                              continue;
                           }
                           newResponseMap.put(entry.getKey(), entry.getValue());
                        }
                        if (containsLastResponseAddress) {
                           newResponseMap.put(lastResponse,
                                              ((Map<Address, Response>) responseObject).get(lastResponse));
                        }
                        log.debugf("Responses for command %s are %s", command, newResponseMap.values());
                        return (T) newResponseMap;
                     });
      }
   }

   private static class ControllerInboundInvocationHandler extends AbstractDelegatingHandler {
      private volatile boolean discardRemoteGet;

      private ControllerInboundInvocationHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (discardRemoteGet && command.getCommandId() == ClusteredGetCommand.COMMAND_ID) {
            return;
         }
         delegate.handle(command, reply, order);
      }
   }

}
