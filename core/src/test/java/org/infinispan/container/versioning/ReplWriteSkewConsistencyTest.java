package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.*;
import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;
import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "container.versioning.ReplWriteSkewConsistencyTest")
@CleanupAfterMethod
public class ReplWriteSkewConsistencyTest extends MultipleCacheManagersTest {

   public void testValidationOnlyInPrimaryOwner() throws Exception {
      final Object key = new MagicKey(cache(1), cache(0));
      final DataContainer primaryOwnerDataContainer = TestingUtil.extractComponent(cache(1), DataContainer.class);
      final DataContainer backupOwnerDataContainer = TestingUtil.extractComponent(cache(0), DataContainer.class);
      final VersionGenerator versionGenerator = TestingUtil.extractComponent(cache(1), VersionGenerator.class);

      injectReorderResponseRpcManager(cache(3), cache(0));
      cache(1).put(key, 1);
      for (Cache cache : caches()) {
         assertEquals("Wrong initial value for cache " + address(cache), 1, cache.get(key));
      }

      InternalCacheEntry ice0 = primaryOwnerDataContainer.get(key);
      InternalCacheEntry ice1 = backupOwnerDataContainer.get(key);
      assertVersion("Wrong version for the same key", ice0.getMetadata().version(), ice1.getMetadata().version(), EQUAL);

      final EntryVersion version0 = ice0.getMetadata().version();
      //version1 is put by tx1
      final EntryVersion version1 = versionGenerator.increment((IncrementableEntryVersion) version0);
      //version2 is put by tx2
      final EntryVersion version2 = versionGenerator.increment((IncrementableEntryVersion) version1);

      ControllerInboundInvocationHandler handler = injectControllerInboundInvocationHandler(cache(0));
      BackupOwnerInterceptor backupOwnerInterceptor = injectBackupOwnerInterceptor(cache(0));
      backupOwnerInterceptor.blockCommit(true);
      handler.discardRemoteGet = true;

      Future<Boolean> tx1 = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            tm(2).begin();
            assertEquals("Wrong value for tx1.", 1, cache(2).get(key));
            cache(2).put(key, 2);
            tm(2).commit();
            return Boolean.TRUE;
         }
      });

      //after this, the primary owner has committed the new value but still have the locks acquired.
      //in the backup owner, it still has the old value

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            Integer value = (Integer) cache(3).get(key);
            return value != null && value == 2;
         }
      });

      assertVersion("Wrong version in the primary owner", primaryOwnerDataContainer.get(key).getMetadata().version(),
                    version1, EQUAL);
      assertVersion("Wrong version in the backup owner", backupOwnerDataContainer.get(key).getMetadata().version(),
                    version0, EQUAL);

      backupOwnerInterceptor.resetPrepare();

      //tx2 will read from the primary owner (i.e., the most recent value) and will commit.
      Future<Boolean> tx2 = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            tm(3).begin();
            assertEquals("Wrong value for tx2.", 2, cache(3).get(key));
            cache(3).put(key, 3);
            tm(3).commit();
            return Boolean.TRUE;
         }
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
      assertVersion("Wrong version in the primary owner", primaryOwnerDataContainer.get(key).getMetadata().version(),
                    version2, EQUAL);
      assertVersion("Wrong version in the backup owner", backupOwnerDataContainer.get(key).getMetadata().version(),
                    version2, EQUAL);


      assertNoTransactions();
      assertNotLocked(key);


   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode(), true);
      builder.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true);
      builder.versioning()
            .enabled(true)
            .scheme(VersioningScheme.SIMPLE);
      builder.clustering().hash().numSegments(60);
      amendConfiguration(builder);
      createClusteredCaches(4, builder);
   }

   protected void amendConfiguration(ConfigurationBuilder builder) {
      //no-op
   }

   protected CacheMode cacheMode() {
      return CacheMode.REPL_SYNC;
   }

   private BackupOwnerInterceptor injectBackupOwnerInterceptor(Cache cache) {
      BackupOwnerInterceptor ownerInterceptor = new BackupOwnerInterceptor();
      cache.getAdvancedCache().addInterceptor(ownerInterceptor, 1);
      return ownerInterceptor;
   }

   private ReorderResponsesRpcManager injectReorderResponseRpcManager(Cache toInject, Cache lastResponse) {
      RpcManager rpcManager = TestingUtil.extractComponent(toInject, RpcManager.class);
      ReorderResponsesRpcManager newRpcManager = new ReorderResponsesRpcManager(address(lastResponse), rpcManager);
      TestingUtil.replaceComponent(toInject, RpcManager.class, newRpcManager, true);
      return newRpcManager;
   }

   private ControllerInboundInvocationHandler injectControllerInboundInvocationHandler(Cache cache) {
      ControllerInboundInvocationHandler handler = new ControllerInboundInvocationHandler(
            extractComponent(cache, InboundInvocationHandler.class));
      replaceComponent(cache.getCacheManager(), InboundInvocationHandler.class, handler, true);
      JGroupsTransport t = (JGroupsTransport) extractComponent(cache, Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      replaceField(handler, "inboundInvocationHandler", card, CommandAwareRpcDispatcher.class);
      return handler;
   }

   private void assertVersion(String message, EntryVersion v0, EntryVersion v1, InequalVersionComparisonResult result) {
      assertTrue(message, v0.compareTo(v1) == result);
   }

   private class BackupOwnerInterceptor extends BaseCustomInterceptor {

      private final Object blockCommitLock = new Object();
      private final Object prepareProcessedLock = new Object();
      private boolean blockCommit;
      private boolean prepareProcessed;

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         blockIfNeeded();
         return invokeNextInterceptor(ctx, command);
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            notifyPrepareProcessed();
         }
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

   private class ReorderResponsesRpcManager extends AbstractControlledRpcManager {

      private final Address lastResponse;

      public ReorderResponsesRpcManager(Address lastResponse, RpcManager realOne) {
         super(realOne);
         this.lastResponse = lastResponse;
      }

      @Override
      protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap) {
         if (responseMap != null) {
            Map<Address, Response> newResponseMap = new LinkedHashMap<Address, Response>();
            boolean containsLastResponseAddress = false;
            for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
               if (lastResponse.equals(entry.getKey())) {
                  containsLastResponseAddress = true;
                  continue;
               }
               newResponseMap.put(entry.getKey(), entry.getValue());
            }
            if (containsLastResponseAddress) {
               newResponseMap.put(lastResponse, responseMap.get(lastResponse));
            }
            log.debugf("Responses for command %s are %s", command, newResponseMap.values());
            return newResponseMap;
         }
         log.debugf("Responses for command %s are null", command);
         return responseMap;
      }
   }

   private class ControllerInboundInvocationHandler implements InboundInvocationHandler {

      private final InboundInvocationHandler realOne;
      private volatile boolean discardRemoteGet;

      private ControllerInboundInvocationHandler(InboundInvocationHandler realOne) {
         this.realOne = realOne;
      }

      @Override
      public void handle(CacheRpcCommand command, Address origin, org.jgroups.blocks.Response response, boolean preserveOrder) throws Throwable {
         if (discardRemoteGet && command.getCommandId() == ClusteredGetCommand.COMMAND_ID) {
            return;
         }
         realOne.handle(command, origin, response, preserveOrder);
      }
   }

}
