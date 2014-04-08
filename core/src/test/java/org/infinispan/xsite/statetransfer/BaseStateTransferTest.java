package org.infinispan.xsite.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverDelegator;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.BackupReceiverRepositoryDelegator;
import org.infinispan.xsite.XSiteAdminOperations;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.*;

/**
 * Tests the cross-site replication with concurrent operations checking for consistency.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BaseStateTransferTest extends AbstractTwoSitesTest {

   protected static final String LON = "LON";
   protected static final String NYC = "NYC";

   public BaseStateTransferTest() {
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testStateTransferNonExistingSite() {
      XSiteAdminOperations operations = extractComponent(cache(LON, 0), XSiteAdminOperations.class);
      assertEquals("Unable to pushState to 'NO_SITE'. Incorrect site name: NO_SITE", operations.pushState("NO_SITE"));
      assertTrue(operations.getRunningStateTransfer().isEmpty());
      assertNoStateTransferInSendingSite(LON);
   }

   public void testStateTransferWithClusterIdle() throws InterruptedException {
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      //NYC is offline... lets put some initial data in
      //we have 2 nodes in each site and the primary owner sends the state. Lets try to have more key than the chunk
      //size in order to each site to send more than one chunk.
      final int amountOfData = chunkSize(LON) * 4;
      for (int i = 0; i < amountOfData; ++i) {
         cache(LON, 0).put(key(i), value(0));
      }

      //check if NYC is empty
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertTrue(cache.isEmpty());
         }
      });

      startStateTransfer(LON, NYC);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertOnline(LON, NYC);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            for (int i = 0; i < amountOfData; ++i) {
               assertEquals(value(0), cache.get(key(i)));
            }
         }
      });

      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);
   }

   public void testPutOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT, true);
   }

   public void testPutOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT, false);
   }

   public void testRemoveOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE, true);
   }

   public void testRemoveOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE, false);
   }

   public void testRemoveIfMatchOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE_IF_MATCH, true);
   }

   public void testRemoveIfMatchOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE_IF_MATCH, false);
   }

   public void testReplaceOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE, true);
   }

   public void testReplaceOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE, false);
   }

   public void testReplaceIfMatchOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE_IF_MATCH, true);
   }

   public void testReplaceIfMatchOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE_IF_MATCH, false);
   }

   public void testClearOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.CLEAR, true);
   }

   public void testClearOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.CLEAR, false);
   }

   public void testPutMapOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT_MAP, true);
   }

   public void testPutMapOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT_MAP, false);
   }

   public void testPutIfAbsentFail() throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.PUT_IF_ABSENT_FAIL);
   }

   public void testRemoveIfMatchFail() throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.REMOVE_IF_MATCH_FAIL);
   }

   public void testReplaceIfMatchFail() throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.REPLACE_IF_MATCH_FAIL);
   }

   public void testPutIfAbsent() throws Exception {
      testConcurrentOperation(Operation.PUT_IF_ABSENT);
   }

   public void testRemoveNonExisting() throws Exception {
      testConcurrentOperation(Operation.REMOVE_NON_EXISTING);
   }

   private void testStateTransferWithConcurrentOperation(final Operation operation, final boolean performBeforeState)
         throws Exception {
      assertNotNull(operation);
      assertTrue(operation.replicates());
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      final Object key = key(0);
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
         public void afterCommand(VisitableCommand command) throws Exception {
            if (performBeforeState && isUpdatingKeyWithValue(command, key, operation.finalValue())) {
               //command was performed before state... let the state continue
               checkPoint.trigger("apply-state");
            }
         }

         @Override
         public void beforeState(XSiteStatePushCommand command) throws Exception {
            checkPoint.trigger("before-state");
            //wait until the command is received with the new value. so we make sure that the command saw the old value
            //and will commit a new value
            checkPoint.awaitStrict("before-update", 30, TimeUnit.SECONDS);
            if (performBeforeState && containsKey(command.getChunk(), key)) {
               //command before state... we need to wait
               checkPoint.awaitStrict("apply-state", 30, TimeUnit.SECONDS);
            }
         }

         @Override
         public void afterState(XSiteStatePushCommand command) throws Exception {
            if (!performBeforeState && containsKey(command.getChunk(), key)) {
               //state before command... let the command go...
               checkPoint.trigger("update-key");
            }
         }
      };

      for (CacheContainer cacheContainer : site(NYC).cacheManagers()) {
         BackupReceiverRepositoryWrapper.replaceInCache(cacheContainer, listener);
      }

      //safe (i.e. not blocking main thread), the state transfer is async
      startStateTransfer(LON, NYC);
      assertOnline(LON, NYC);

      //state transfer should send old value
      checkPoint.awaitStrict("before-state", 30, TimeUnit.SECONDS);


      //safe, perform is async
      operation.perform(cache(LON, 0), key).get();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertEventuallyNoStateTransferInReceivingSite(NYC, 30, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite(LON, 30, TimeUnit.SECONDS);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operation.finalValue(), cache.get(key));
         }
      });
      assertInSite(LON, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operation.finalValue(), cache.get(key));
         }
      });
   }

   private void testConcurrentOperation(final Operation operation) throws Exception {
      assertNotNull(operation);
      assertTrue(operation.replicates());
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      final Object key = key(0);

      operation.init(cache(LON, 0), key);
      assertNull(operation.initialValue());

      final XSiteStateProviderControl control = XSiteStateProviderControl.replaceInCache(cache(LON, 0));

      //safe (i.e. not blocking main thread), the state transfer is async
      final Future<?> f = fork(new Runnable() {
         @Override
         public void run() {
            startStateTransfer(LON, NYC);
         }
      });

      //state transfer will be running (nothing to transfer however) while the operation is done.
      control.await();
      assertOnline(LON, NYC);

      //safe, perform is async
      operation.perform(cache(LON, 0), key).get();

      control.trigger();
      f.get(30, TimeUnit.SECONDS);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertEventuallyNoStateTransferInReceivingSite(NYC, 30, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite(LON, 30, TimeUnit.SECONDS);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operation.finalValue(), cache.get(key));
         }
      });
      assertInSite(LON, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operation.finalValue(), cache.get(key));
         }
      });
   }

   private void testStateTransferWithNoReplicatedOperation(final Operation operation) throws Exception {
      assertNotNull(operation);
      assertFalse(operation.replicates());
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      final Object key = key(0);
      final CheckPoint checkPoint = new CheckPoint();
      final AtomicBoolean commandReceived = new AtomicBoolean(false);

      operation.init(cache(LON, 0), key);
      assertNotNull(operation.initialValue());

      final BackupListener listener = new BackupListener() {
         @Override
         public void beforeCommand(VisitableCommand command) throws Exception {
            commandReceived.set(true);
         }

         @Override
         public void afterCommand(VisitableCommand command) throws Exception {
            commandReceived.set(true);
         }

         @Override
         public void beforeState(XSiteStatePushCommand command) throws Exception {
            checkPoint.trigger("before-state");
            checkPoint.awaitStrict("before-update", 30, TimeUnit.SECONDS);
         }
      };

      for (CacheContainer cacheContainer : site(NYC).cacheManagers()) {
         BackupReceiverRepositoryWrapper.replaceInCache(cacheContainer, listener);
      }

      //safe (i.e. not blocking main thread), the state transfer is async
      startStateTransfer(LON, NYC);
      assertOnline(LON, NYC);

      //state transfer should send old value
      checkPoint.awaitStrict("before-state", 30, TimeUnit.SECONDS);

      //safe, perform is async
      operation.perform(cache(LON, 0), key).get();

      assertFalse(commandReceived.get());
      checkPoint.trigger("before-update");

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertEventuallyNoStateTransferInReceivingSite(NYC, 30, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite(LON, 30, TimeUnit.SECONDS);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operation.finalValue(), cache.get(key));
         }
      });
      assertInSite(LON, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operation.finalValue(), cache.get(key));
         }
      });
   }

   private boolean isUpdatingKeyWithValue(VisitableCommand command, Object key, Object value) {
      if (command instanceof PutKeyValueCommand) {
         return key.equals(((PutKeyValueCommand) command).getKey()) &&
               value.equals(((PutKeyValueCommand) command).getValue());
      } else if (command instanceof RemoveCommand) {
         return key.equals(((RemoveCommand) command).getKey());
      } else if (command instanceof ReplaceCommand) {
         return key.equals(((ReplaceCommand) command).getKey()) &&
               value.equals(((ReplaceCommand) command).getNewValue());
      } else if (command instanceof ClearCommand) {
         return true;
      } else if (command instanceof PutMapCommand) {
         return ((PutMapCommand) command).getMap().containsKey(key) &&
               ((PutMapCommand) command).getMap().get(key).equals(value);
      } else if (command instanceof PrepareCommand) {
         for (WriteCommand writeCommand : ((PrepareCommand) command).getModifications()) {
            if (isUpdatingKeyWithValue(writeCommand, key, value)) {
               return true;
            }
         }
      }
      return false;
   }

   private boolean containsKey(XSiteState[] states, Object key) {
      if (states == null || states.length == 0 || key == null) {
         return false;
      }
      for (XSiteState state : states) {
         if (key.equals(state.key())) {
            return true;
         }
      }
      return false;
   }

   private void startStateTransfer(String fromSite, String toSite) {
      XSiteAdminOperations operations = extractComponent(cache(fromSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.pushState(toSite));
   }

   private void takeSiteOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.takeSiteOffline(remoteSite));
   }

   private void assertOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.OFFLINE, operations.siteStatus(remoteSite));
   }

   private void assertOnline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.ONLINE, operations.siteStatus(remoteSite));
   }

   private int chunkSize(String site) {
      return cache(site, 0).getCacheConfiguration().sites().allBackups().get(0).stateTransfer().chunkSize();
   }

   private Object key(int index) {
      return "key-" + index;
   }

   private Object value(int index) {
      return "value-" + index;
   }

   private void assertNoStateTransferInReceivingSite(String siteName) {
      assertInSite(siteName, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            CommitManager commitManager = extractComponent(cache, CommitManager.class);
            assertFalse(commitManager.isTracking(Flag.PUT_FOR_STATE_TRANSFER));
            assertFalse(commitManager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
            assertTrue(commitManager.isEmpty());
         }
      });
   }

   private void assertEventuallyNoStateTransferInReceivingSite(String siteName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            CommitManager commitManager = extractComponent(cache, CommitManager.class);
            return !commitManager.isTracking(Flag.PUT_FOR_STATE_TRANSFER) &&
                  !commitManager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER) &&
                  commitManager.isEmpty();
         }
      }, timeout, unit);
   }

   private void assertNoStateTransferInSendingSite(String siteName) {
      assertInSite(siteName, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertTrue(extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty());
         }
      });
   }

   private void assertEventuallyNoStateTransferInSendingSite(String siteName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            return extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty();
         }
      }, timeout, unit);
   }

   private static enum Operation {
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

      public abstract <K> void init(Cache<K, Object> cache, K key);

      public abstract <K> Future<?> perform(Cache<K, Object> cache, K key);

      public abstract boolean replicates();

      public final Object initialValue() {
         return initialValue;
      }

      public final Object finalValue() {
         return finalValue;
      }
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

      public final void await() throws TimeoutException, InterruptedException {
         checkPoint.awaitStrict("before-start", 30, TimeUnit.SECONDS);
      }

      public final void trigger() {
         checkPoint.trigger("await-start");
      }

      public static XSiteStateProviderControl replaceInCache(Cache<?, ?> cache) {
         XSiteStateProvider current = extractComponent(cache, XSiteStateProvider.class);
         XSiteStateProviderControl control = new XSiteStateProviderControl(current);
         replaceComponent(cache, XSiteStateProvider.class, control, true);
         return control;
      }
   }

   private static class BackupReceiverRepositoryWrapper extends BackupReceiverRepositoryDelegator {

      private final BackupListener listener;

      public BackupReceiverRepositoryWrapper(BackupReceiverRepository delegate, BackupListener listener) {
         super(delegate);
         if (listener == null) {
            throw new NullPointerException("Listener must not be null.");
         }
         this.listener = listener;
      }

      @Override
      public BackupReceiver getBackupReceiver(String originSiteName, String cacheName) {
         return new BackupReceiverDelegator(super.getBackupReceiver(originSiteName, cacheName)) {
            @Override
            public Object handleRemoteCommand(VisitableCommand command) throws Throwable {
               listener.beforeCommand(command);
               try {
                  return super.handleRemoteCommand(command);
               } finally {
                  listener.afterCommand(command);
               }
            }

            @Override
            public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
               listener.beforeState(cmd);
               try {
                  super.handleStateTransferState(cmd);
               } finally {
                  listener.afterState(cmd);
               }
            }
         };
      }

      public static BackupReceiverRepositoryWrapper replaceInCache(CacheContainer cacheContainer, BackupListener listener) {
         BackupReceiverRepository delegate = extractGlobalComponent(cacheContainer, BackupReceiverRepository.class);
         BackupReceiverRepositoryWrapper wrapper = new BackupReceiverRepositoryWrapper(delegate, listener);
         replaceComponent(cacheContainer, BackupReceiverRepository.class, wrapper, true);
         JGroupsTransport t = (JGroupsTransport) extractGlobalComponent(cacheContainer, Transport.class);
         CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
         replaceField(wrapper, "backupReceiverRepository", card, CommandAwareRpcDispatcher.class);
         return wrapper;
      }
   }

   private static abstract class BackupListener {

      public void beforeCommand(VisitableCommand command) throws Exception {
         //no-op by default
      }

      public void afterCommand(VisitableCommand command) throws Exception {
         //no-op by default
      }

      public void beforeState(XSiteStatePushCommand command) throws Exception {
         //no-op by default
      }

      public void afterState(XSiteStatePushCommand command) throws Exception {
         //no-op by default
      }

   }
}
