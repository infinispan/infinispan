package org.infinispan.xsite.statetransfer;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.infinispan.xsite.XSiteAdminOperations.SUCCESS;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferManager.STATUS_CANCELED;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferManager.STATUS_SENDING;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.test.ExceptionRunnable;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.ControlledTransport;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverDelegator;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.commands.XSiteStateTransferCancelSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStatusRequestCommand;
import org.infinispan.xsite.commands.remote.XSiteStatePushRequest;
import org.infinispan.xsite.commands.remote.XSiteStateTransferControlRequest;
import org.testng.annotations.Test;

/**
 * Tests the cross-site replication with concurrent operations checking for consistency.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BaseStateTransferTest extends AbstractStateTransferTest {

   private static final String VALUE = "value";

   BaseStateTransferTest() {
      this.cleanup = CleanupPhase.AFTER_METHOD;
      this.cacheMode = CacheMode.DIST_SYNC;
   }

   @Test(groups = "xsite")
   public void testStateTransferNonExistingSite() {
      XSiteAdminOperations operations = adminOperations();
      assertEquals("Unable to pushState to 'NO_SITE'. Incorrect site name: NO_SITE", operations.pushState("NO_SITE"));
      assertTrue(operations.getRunningStateTransfer().isEmpty());
      assertNoStateTransferInSendingSite();
   }

   @Test(groups = "xsite")
   public void testCancelStateTransfer(Method method) throws InterruptedException {
      takeSiteOffline();
      assertOffline();
      assertNoStateTransferInReceivingSite(null);
      assertNoStateTransferInSendingSite();

      // NYC is offline... lets put some initial data in LON.
      // The primary owner is the one sending the state to the backup.
      // We add keys until we have more than one chunk on the LON coordinator.
      LocalizedCacheTopology topology = cache(LON, 0).getAdvancedCache().getDistributionManager().getCacheTopology();
      Address coordLON = cache(LON, 0).getCacheManager().getAddress();
      Set<Object> keysOnCoordinator = new HashSet<>();
      int i = 0;
      while (keysOnCoordinator.size() < chunkSize()) {
         Object key = k(method, i);
         cache(LON, 0).put(key, VALUE);
         if (topology.getDistribution(key).primary().equals(coordLON)) {
            keysOnCoordinator.add(key);
         }
         ++i;
      }
      int numKeys = i;
      log.debugf("Coordinator %s is primary owner for %d keys: %s", coordLON, keysOnCoordinator.size(), keysOnCoordinator);

      //check if NYC is empty
      assertInSite(NYC, cache -> assertTrue(cache.isEmpty()));

      ControlledTransport controlledTransport = ControlledTransport.replace(cache(LON, 0));
      controlledTransport.excludeCommands(
            XSiteStateTransferStartReceiveCommand.class,
            XSiteStateTransferControlRequest.class,
            XSiteStateTransferStartSendCommand.class,
            XSiteStateTransferCancelSendCommand.class,
            XSiteStateTransferFinishReceiveCommand.class,
            XSiteStateTransferStatusRequestCommand.class);
      controlledTransport.excludeCacheCommands();

      startStateTransfer();

      // Wait for a push command and block it
      ControlledTransport.BlockedRequest<XSiteStatePushRequest> pushRequest =
            controlledTransport.expectCommand(XSiteStatePushRequest.class);

      assertEquals(SUCCESS, adminOperations().cancelPushState(NYC));

      // Unblock the push command
      pushRequest.send().receiveAll();

      assertEventuallyStateTransferNotRunning();

      assertEventuallyNoStateTransferInReceivingSite(null);
      assertEventuallyNoStateTransferInSendingSite();

      assertEquals(STATUS_CANCELED, adminOperations().getPushStateStatus().get(NYC));


      startStateTransfer();

      // Wait for a push command and block it
      ControlledTransport.BlockedRequest<XSiteStatePushRequest> pushRequest2 =
            controlledTransport.expectCommand(XSiteStatePushRequest.class);

      assertEquals(STATUS_SENDING, adminOperations().getPushStateStatus().get(NYC));

      // Unblock the push command
      pushRequest2.send().receiveAll();

      assertEventuallyStateTransferNotRunning();

      assertEventuallyNoStateTransferInReceivingSite(null);
      assertEventuallyNoStateTransferInSendingSite();

      //check if all data is visible
      assertInSite(NYC, cache -> {
         for (int i1 = 0; i1 < numKeys; ++i1) {
            assertEquals(VALUE, cache.get(k(method, i1)));
         }
      });

      controlledTransport.stopBlocking();
   }

   @Test(groups = "xsite")
   public void testStateTransferWithClusterIdle(Method method) {
      takeSiteOffline();
      assertOffline();
      assertNoStateTransferInReceivingSite(null);
      assertNoStateTransferInSendingSite();

      //NYC is offline... lets put some initial data in
      //we have 2 nodes in each site and the primary owner sends the state. Lets try to have more key than the chunk
      //size in order to each site to send more than one chunk.
      final int amountOfData = chunkSize() * 4;
      for (int i = 0; i < amountOfData; ++i) {
         cache(LON, 0).put(k(method, i), VALUE);
      }

      //check if NYC is empty
      assertInSite(NYC, cache -> assertTrue(cache.isEmpty()));

      startStateTransfer();

      assertEventuallyStateTransferNotRunning();

      assertOnline(LON, NYC);

      //check if all data is visible
      assertInSite(NYC, cache -> {
         for (int i = 0; i < amountOfData; ++i) {
            assertEquals(VALUE, cache.get(k(method, i)));
         }
      });

      assertEventuallyNoStateTransferInReceivingSite(null);
      assertEventuallyNoStateTransferInSendingSite();
   }

   @Test(groups = "xsite")
   public void testPutOperationBeforeState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT, true, method);
   }

   @Test(groups = "xsite")
   public void testPutOperationAfterState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT, false, method);
   }

   @Test(groups = "xsite")
   public void testRemoveOperationBeforeState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE, true, method);
   }

   @Test(groups = "xsite")
   public void testRemoveOperationAfterState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE, false, method);
   }

   @Test(groups = "xsite")
   public void testRemoveIfMatchOperationBeforeState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE_IF_MATCH, true, method);
   }

   @Test(groups = "xsite")
   public void testRemoveIfMatchOperationAfterState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE_IF_MATCH, false, method);
   }

   @Test(groups = "xsite")
   public void testReplaceOperationBeforeState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE, true, method);
   }

   @Test(groups = "xsite")
   public void testReplaceOperationAfterState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE, false, method);
   }

   @Test(groups = "xsite")
   public void testReplaceIfMatchOperationBeforeState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE_IF_MATCH, true, method);
   }

   @Test(groups = "xsite")
   public void testReplaceIfMatchOperationAfterState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE_IF_MATCH, false, method);
   }

   @Test(groups = "xsite")
   public void testClearOperationBeforeState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.CLEAR, true, method);
   }

   @Test(groups = "xsite")
   public void testClearOperationAfterState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.CLEAR, false, method);
   }

   @Test(groups = "xsite")
   public void testPutMapOperationBeforeState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT_MAP, true, method);
   }

   @Test(groups = "xsite")
   public void testPutMapOperationAfterState(Method method) throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT_MAP, false, method);
   }

   @Test(groups = "xsite")
   public void testPutIfAbsentFail(Method method) throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.PUT_IF_ABSENT_FAIL, method);
   }

   @Test(groups = "xsite")
   public void testRemoveIfMatchFail(Method method) throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.REMOVE_IF_MATCH_FAIL, method);
   }

   @Test(groups = "xsite")
   public void testReplaceIfMatchFail(Method method) throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.REPLACE_IF_MATCH_FAIL, method);
   }

   @Test(groups = "xsite")
   public void testPutIfAbsent(Method method) throws Exception {
      testConcurrentOperation(Operation.PUT_IF_ABSENT, method);
   }

   @Test(groups = "xsite")
   public void testRemoveNonExisting(Method method) throws Exception {
      testConcurrentOperation(Operation.REMOVE_NON_EXISTING, method);
   }

   @Override
   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      builder.stateTransfer().chunkSize(2).timeout(2000);
   }

   private void testStateTransferWithConcurrentOperation(final Operation operation, final boolean performBeforeState,
         final Method method) throws Exception {
      assertNotNull(operation);
      assertTrue(operation.replicates());
      takeSiteOffline();
      assertOffline();
      assertNoStateTransferInReceivingSite(null);
      assertNoStateTransferInSendingSite();

      final Object key = k(method, 0);
      final CheckPoint checkPoint = new CheckPoint();

      operation.init(cache(LON, 0), key);
      assertNotNull(operation.initialValue());

      final BackupListener listener = new BackupListener() {
         @Override
         public void beforeCommand(VisitableCommand command) throws Exception {
            checkPoint.trigger("before-update");
            if (!performBeforeState && isUpdatingKeyWithValue(command, key, operation.finalValue())) {
               //we need to wait for the state transfer before perform the command
               checkPoint.awaitStrict("update-key", 30, TimeUnit.SECONDS);
            }
         }

         @Override
         public void afterCommand(VisitableCommand command) {
            if (performBeforeState && isUpdatingKeyWithValue(command, key, operation.finalValue())) {
               //command was performed before state... let the state continue
               checkPoint.trigger("apply-state");
            }
         }

         @Override
         public void beforeState(List<XSiteState> chunk) throws Exception {
            checkPoint.trigger("before-state");
            //wait until the command is received with the new value. so we make sure that the command saw the old value
            //and will commit a new value
            checkPoint.awaitStrict("before-update", 30, TimeUnit.SECONDS);
            if (performBeforeState && containsKey(chunk, key)) {
               //command before state... we need to wait
               checkPoint.awaitStrict("apply-state", 30, TimeUnit.SECONDS);
            }
         }

         @Override
         public void afterState(List<XSiteState> chunk) {
            if (!performBeforeState && containsKey(chunk, key)) {
               //state before command... let the command go...
               checkPoint.trigger("update-key");
            }
         }
      };

      for (Cache<?, ?> cache : caches(NYC)) {
         wrapComponent(cache, BackupReceiver.class, current -> new ListenableBackupReceiver(current, listener));
      }

      //safe (i.e. not blocking main thread), the state transfer is async
      startStateTransfer();
      assertOnline(LON, NYC);

      //state transfer should send old value
      checkPoint.awaitStrict("before-state", 30, TimeUnit.SECONDS);


      //safe, perform is async
      operation.perform(cache(LON, 0), key).get();

      assertEventuallyStateTransferNotRunning();

      assertEventuallyNoStateTransferInReceivingSite(null);
      assertEventuallyNoStateTransferInSendingSite();

      //check if all data is visible
      assertInSite(NYC, cache -> assertEquals(operation.finalValue(), cache.get(key)));
      assertInSite(LON, cache -> assertEquals(operation.finalValue(), cache.get(key)));
   }

   private void testConcurrentOperation(final Operation operation, final Method method) throws Exception {
      assertNotNull(operation);
      assertTrue(operation.replicates());
      takeSiteOffline();
      assertOffline();
      assertNoStateTransferInReceivingSite(null);
      assertNoStateTransferInSendingSite();

      final Object key = k(method, 0);

      operation.init(cache(LON, 0), key);
      assertNull(operation.initialValue());

      final XSiteStateProviderControl control = XSiteStateProviderControl.replaceInCache(cache(LON, 0));

      //safe (i.e. not blocking main thread), the state transfer is async
      final Future<?> f = fork((ExceptionRunnable) this::startStateTransfer);

      //state transfer will be running (nothing to transfer however) while the operation is done.
      control.await();
      assertOnline(LON, NYC);

      //safe, perform is async
      operation.perform(cache(LON, 0), key).get();

      control.trigger();
      f.get(30, TimeUnit.SECONDS);

      assertEventuallyStateTransferNotRunning();

      assertEventuallyNoStateTransferInReceivingSite(null);
      assertEventuallyNoStateTransferInSendingSite();

      //check if all data is visible
      assertInSite(NYC, cache -> assertEquals(operation.finalValue(), cache.get(key)));
      assertInSite(LON, cache -> assertEquals(operation.finalValue(), cache.get(key)));
   }

   private void testStateTransferWithNoReplicatedOperation(final Operation operation, final Method method)
         throws Exception {
      assertNotNull(operation);
      assertFalse(operation.replicates());
      takeSiteOffline();
      assertOffline();
      assertNoStateTransferInReceivingSite(null);
      assertNoStateTransferInSendingSite();

      final Object key = k(method, 0);
      final CheckPoint checkPoint = new CheckPoint();
      final AtomicBoolean commandReceived = new AtomicBoolean(false);

      operation.init(cache(LON, 0), key);
      assertNotNull(operation.initialValue());

      final BackupListener listener = new BackupListener() {
         @Override
         public void beforeCommand(VisitableCommand command) {
            commandReceived.set(true);
         }

         @Override
         public void afterCommand(VisitableCommand command) {
            commandReceived.set(true);
         }

         @Override
         public void beforeState(List<XSiteState> chunk) throws Exception {
            checkPoint.trigger("before-state");
            checkPoint.awaitStrict("before-update", 30, TimeUnit.SECONDS);
         }
      };

      for (Cache<?, ?> cache : caches(NYC)) {
         wrapComponent(cache, BackupReceiver.class, current -> new ListenableBackupReceiver(current, listener));
      }

      //safe (i.e. not blocking main thread), the state transfer is async
      startStateTransfer();
      assertOnline(LON, NYC);

      //state transfer should send old value
      checkPoint.awaitStrict("before-state", 30, TimeUnit.SECONDS);

      //safe, perform is async
      operation.perform(cache(LON, 0), key).get();

      assertFalse(commandReceived.get());
      checkPoint.trigger("before-update");

      assertEventuallyStateTransferNotRunning();

      assertEventuallyNoStateTransferInReceivingSite(null);
      assertEventuallyNoStateTransferInSendingSite();

      //check if all data is visible
      assertInSite(NYC, cache -> assertEquals(operation.finalValue(), cache.get(key)));
      assertInSite(LON, cache -> assertEquals(operation.finalValue(), cache.get(key)));
   }

   private boolean isUpdatingKeyWithValue(VisitableCommand command, Object key, Object value) {
      if (command instanceof PutKeyValueCommand) {
         return key.equals(((PutKeyValueCommand) command).getKey()) &&
               value.equals(((PutKeyValueCommand) command).getValue());
      } else if (command instanceof RemoveCommand) {
         return key.equals(((RemoveCommand) command).getKey());
      } else if (command instanceof ClearCommand) {
         return true;
      } else if (command instanceof WriteOnlyManyEntriesCommand) {
         InternalCacheValue<?> icv = (InternalCacheValue<?>) ((WriteOnlyManyEntriesCommand<?, ?, ?>) command).getArguments().get(key);
         return Objects.equals(icv.getValue(), value);
      } else if (command instanceof PrepareCommand) {
         for (WriteCommand writeCommand : ((PrepareCommand) command).getModifications()) {
            if (isUpdatingKeyWithValue(writeCommand, key, value)) {
               return true;
            }
         }
      }
      return false;
   }

   private boolean containsKey(List<XSiteState> states, Object key) {
      if (states == null || states.isEmpty() || key == null) {
         return false;
      }
      for (XSiteState state : states) {
         if (key.equals(state.key())) {
            return true;
         }
      }
      return false;
   }

   private enum Operation {
      PUT("v0", "v1") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.putAsync(key, finalValue());
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      PUT_IF_ABSENT(null, "v1") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            //no-op
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.putIfAbsentAsync(key, finalValue());
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      PUT_IF_ABSENT_FAIL("v0", "v0") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.putIfAbsentAsync(key, "v1");
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      REPLACE("v0", "v1") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.replaceAsync(key, finalValue());
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      /**
       * not used: it has no state to transfer neither it is replicated! can be useful in the future.
       */
      REPLACE_NON_EXISTING(null, null) {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            //no-op
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.replaceAsync(key, "v1");
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      REPLACE_IF_MATCH("v0", "v1") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.replaceAsync(key, initialValue(), finalValue());
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REPLACE_IF_MATCH_FAIL("v0", "v0") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            //this works because value != initial value, so the match will fail.
            return cache.replaceAsync(key, "v1", "v1");
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      REMOVE_NON_EXISTING(null, null) {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            //no-op
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.removeAsync(key);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REMOVE("v0", null) {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.removeAsync(key);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REMOVE_IF_MATCH("v0", null) {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.removeAsync(key, initialValue());
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REMOVE_IF_MATCH_FAIL("v0", "v0") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            //this works because value != initial value, so the match will fail.
            return cache.removeAsync(key, "v1");
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      CLEAR("v0", null) {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            return cache.clearAsync();
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      PUT_MAP("v0", "v1") {
         @Override
         public <K> void init(Cache<K, Object> cache, K key) {
            cache.put(key, initialValue());
         }

         @Override
         public <K> Future<?> perform(Cache<K, Object> cache, K key) {
            Map<K, Object> map = new HashMap<>();
            map.put(key, finalValue());
            return cache.putAllAsync(map);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      };

      private final Object initialValue;
      private final Object finalValue;

      Operation(Object initialValue, Object finalValue) {
         this.initialValue = initialValue;
         this.finalValue = finalValue;
      }

      final Object initialValue() {
         return initialValue;
      }

      final Object finalValue() {
         return finalValue;
      }

      protected abstract <K> void init(Cache<K, Object> cache, K key);

      protected abstract <K> Future<?> perform(Cache<K, Object> cache, K key);

      protected abstract boolean replicates();
   }

   private static class XSiteStateProviderControl extends XSiteProviderDelegator {

      private final CheckPoint checkPoint;

      private XSiteStateProviderControl(XSiteStateProvider xSiteStateProvider) {
         super(xSiteStateProvider);
         checkPoint = new CheckPoint();
      }

      @Override
      public void startStateTransfer(String siteName, Address requestor, int minTopologyId) {
         checkPoint.trigger("before-start");
         try {
            checkPoint.awaitStrict("await-start", 30, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
         } catch (TimeoutException e) {
            throw new RuntimeException(e);
         }
         super.startStateTransfer(siteName, requestor, minTopologyId);
      }

      static XSiteStateProviderControl replaceInCache(Cache<?, ?> cache) {
         XSiteStateProvider current = extractComponent(cache, XSiteStateProvider.class);
         XSiteStateProviderControl control = new XSiteStateProviderControl(current);
         replaceComponent(cache, XSiteStateProvider.class, control, true);
         return control;
      }

      final void await() throws TimeoutException, InterruptedException {
         checkPoint.awaitStrict("before-start", 30, TimeUnit.SECONDS);
      }

      final void trigger() {
         checkPoint.trigger("await-start");
      }
   }

   private static abstract class BackupListener {

      void beforeCommand(VisitableCommand command) throws Exception {
         //no-op by default
      }

      void afterCommand(VisitableCommand command) {
         //no-op by default
      }

      void beforeState(List<XSiteState> chunk) throws Exception {
         //no-op by default
      }

      void afterState(List<XSiteState> chunk) {
         //no-op by default
      }
   }

   private static class ListenableBackupReceiver extends BackupReceiverDelegator {

      private final BackupListener listener;

      ListenableBackupReceiver(BackupReceiver delegate, BackupListener listener) {
         super(delegate);
         this.listener = Objects.requireNonNull(listener, "Listener must not be null.");
      }

      @Override
      public <O> CompletionStage<O> handleRemoteCommand(VisitableCommand command) {
         try {
            listener.beforeCommand(command);
         } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
         }
         return super.<O>handleRemoteCommand(command).whenComplete((v, t) -> listener.afterCommand(command));
      }

      @Override
      public CompletionStage<Void> handleStateTransferState(List<XSiteState> chunk, long timeoutMs) {
         try {
            listener.beforeState(chunk);
         } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
         }
         return super.handleStateTransferState(chunk, timeoutMs).whenComplete((v, t) -> listener.afterState(chunk));
      }
   }
}
