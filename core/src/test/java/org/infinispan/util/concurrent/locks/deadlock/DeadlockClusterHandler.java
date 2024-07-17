package org.infinispan.util.concurrent.locks.deadlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.join;
import static org.testng.AssertJUnit.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

public class DeadlockClusterHandler {

   public interface TestLeech {

      <K, V> Cache<K, V> cache(int node);

      Address address(int node);

      <T> Future<T> fork(Callable<T> callable);

      String keyGenerator(String prefix, int node);

      void eventually(ThrowingBooleanSupplier tbs);
   }

   public interface ThrowingBooleanSupplier {
      boolean getAsBoolean() throws Exception;
   }

   private final TestLeech test;
   private final Map<String, String> payload;
   private final Future<DeadlockOperation>[] runningTransactions;
   private final GlobalTransaction[] transactions;
   private final DeadlockTestInterceptor[] interceptors;

   private DeadlockClusterHandler(TestLeech test, Map<String, String> payload, int size) {
      this.test = test;
      this.payload = payload;
      this.runningTransactions = new Future[size];
      this.transactions = new GlobalTransaction[size];
      this.interceptors = new DeadlockTestInterceptor[size];
   }

   public static DeadlockClusterHandler create(TestLeech test, int ... nodes) throws Throwable {
      Map<String, String> payload = createData(test, nodes);
      DeadlockClusterHandler dch = new DeadlockClusterHandler(test, payload, nodes.length);

      for (int i = 0; i < nodes.length; i++) {
         int node = nodes[i];
         PerCacheInboundInvocationHandler pciih = TestingUtil.extractComponent(test.cache(node), PerCacheInboundInvocationHandler.class);
         DeadlockTestInterceptor interceptor;
         if (pciih instanceof DeadlockTestInterceptor dti) {
            interceptor = dti;
         } else {
            interceptor = TestingUtil.wrapInboundInvocationHandler(test.cache(node), DeadlockTestInterceptor::new);
         }
         dch.interceptors[i] = interceptor;
      }


      for (int i = 0; i < nodes.length; i++) {
         int node = nodes[i];
         CompletableFuture<GlobalTransaction> cf = new CompletableFuture<>();
         dch.runningTransactions[i] = runTransaction(test, node, payload, cf);
         dch.transactions[i] = join(cf);
      }

      // Assert the whole cluster receive the message from all other nodes;
      for (int i = 0; i < nodes.length; i++) {
         int node = nodes[i];
         for (int j = 0; j < nodes.length; j++) {
            int sender = nodes[j];
            if (node == sender) continue;

            LockCommandCheckPoint checkPoint = dch.lockCommandCheckPoint(i);
            checkPoint.awaitStartLockCommand(test.address(j), dch.transaction(j));
         }
      }

      return dch;
   }

   public static DeadlockClusterHandler create(TestLeech test, Map<Integer, Integer> dependency, int ... nodes) throws Throwable {
      DeadlockClusterHandler dch = create(test, nodes);
      dch.allowLockCommands(dependency);
      return dch;
   }

   public LockCommandCheckPoint lockCommandCheckPoint(int i) {
      return interceptors[i].lockCheckpoint;
   }

   public DeadlockCommandCheckpoint deadlockCheckpoint(int i) {
      return interceptors[i].deadlockCheckpoint;
   }

   public GlobalTransaction transaction(int i) {
      return transactions[i];
   }

   public Future<DeadlockOperation> runningOperation(int i) {
      return runningTransactions[i];
   }

   public Address address(int i) {
      return test.address(i);
   }

   public void assertTransactionsNotCompleted(int ... nodes) {
      for (int node : nodes) {
         TransactionTable table = TestingUtil.getTransactionTable(test.cache(node));

         LocalTransaction tx = table.getLocalTransaction(transaction(node));
         int status = -1;
         try {
            status = tx.getTransaction().getStatus();
         } catch (SystemException ignore) { }

         assertThat(isDone(status)).isFalse();
      }
   }

   public Set<String> keySet() {
      return payload.keySet();
   }

   public boolean isTransactionsCompleted(int ... nodes) {
      for (int node : nodes) {
         Future<?> f = runningOperation(node);
         if (!f.isDone())
            return false;
      }

      return true;
   }

   public void assertLockOwner(int node, int owner) {
      LockManager manager = TestingUtil.extractComponent(test.cache(node), LockManager.class);
      String key = getOwnedKey(node, payload);
      GlobalTransaction expected = transaction(owner);
      Object actual = manager.getOwner(key);
      assertThat(actual)
            .withFailMessage(() -> String.format("%d (%s):\nExpected <%s>\nActual: <%s>\n\n%s", node, key, expected, actual, showKeyOwners()))
            .isEqualTo(expected);
   }

   public void releaseAllDeadlockCommands() {
      for (DeadlockTestInterceptor interceptor : interceptors) {
         interceptor.deadlockCheckpoint.allowAll.set(true);
      }

      for (GlobalTransaction initiator : transactions) {
         for (GlobalTransaction holder : transactions) {
            for (int i = 0; i < transactions.length; i++) {
               deadlockCheckpoint(i).allowDeadlockCommand(initiator, holder);
            }
         }
      }
   }

   public void waitForDeadlockCommand() {
      ThrowingBooleanSupplier bs = () -> {
         for (GlobalTransaction initiator : transactions) {
            for (GlobalTransaction holder : transactions) {
               for (DeadlockTestInterceptor interceptor : interceptors) {
                  if (interceptor.deadlockCheckpoint.hasAnyOf(initiator, holder))
                     return true;
               }
            }
         }
         return false;
      };
      test.eventually(bs);
   }

   public void releaseAllLockCommands(int ... nodes) {
      for (int node : nodes) {
         for (int sender : nodes) {
            lockCommandCheckPoint(node).allowLockCommand(test.address(sender), transaction(sender));
         }
      }
   }

   public String getKeyOwned(int node) {
      return getOwnedKey(node, payload);
   }

   public void allowLockCommands(Map<Integer, Integer> dependency) {
      // Allow to acquire locks following the dependency graph.
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         int node = entry.getKey();
         int holder = entry.getValue();

         LockCommandCheckPoint checkPoint = lockCommandCheckPoint(node);
         checkPoint.proceedLockCommand(test.address(holder), transaction(holder));
         checkPoint.awaitDoneLockCommand(test.address(holder), transaction(holder));
      }
   }

   public String showKeyOwners() {
      Collection<?> keys = payload.keySet();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < transactions.length; i++) {
         LockManager manager = TestingUtil.extractComponent(test.cache(i), LockManager.class);
         sb.append("At node: ").append(i).append(System.lineSeparator());
         for (Object key : keys) {
            sb.append("key: ").append(key)
                  .append("; owner: ").append(manager.getOwner(key))
                  .append(" (").append(manager.getPendingOwners(key)).append(")")
                  .append(System.lineSeparator());
         }
         sb.append(System.lineSeparator());
      }
      return sb.toString();
   }

   public void assertOperationsFinish(boolean guaranteeExecution) throws Throwable {
      int success = 0;
      ThrowingBooleanSupplier bs = () -> {
         for (Future<DeadlockOperation> op : runningTransactions) {
            if (!op.isDone())
               return false;
         }
         return true;
      };
      test.eventually(bs);

      for (Future<DeadlockOperation> op : runningTransactions) {
         assertThat(op.isDone()).isTrue();
         DeadlockOperation res = op.get(10, TimeUnit.SECONDS);
         if (res.isSuccess()) {
            success++;
            continue;
         }

         assertThat(res.throwable())
               .satisfiesAnyOf(
                     // Failed because of a deadlock was detected.
                     t -> assertThat(t).hasRootCauseInstanceOf(DeadlockDetectedException.class),
                     // Because the operation roll back.
                     t -> assertThat(t).isInstanceOf(RollbackException.class),
                     // Operating on a rolled transaction.
                     t -> assertThat(t).hasMessageContaining("ActionStatus.ABORT_ONLY"),
                     // Failed because of partition handling.
                     t -> assertThat(t).hasRootCauseInstanceOf(AvailabilityException.class));
      }

      if (guaranteeExecution)
         assertThat(success).isNotZero();
   }

   private static class DeadlockTestInterceptor extends AbstractDelegatingHandler {
      private final LockCommandCheckPoint lockCheckpoint;
      private final DeadlockCommandCheckpoint deadlockCheckpoint;

      protected DeadlockTestInterceptor(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
         this.lockCheckpoint = new LockCommandCheckPoint(delegate);
         this.deadlockCheckpoint = new DeadlockCommandCheckpoint(delegate);
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof LockControlCommand lcc) {
            lockCheckpoint.handleCommand(lcc, reply, order);
         } else if (command instanceof DeadlockProbeCommand dpc) {
            deadlockCheckpoint.handleCommand(dpc, reply, order);
         } else {
            delegate.handle(command, reply, order);
         }
      }
   }

   private static abstract class CommandCheckpoint<T extends CacheRpcCommand> {
      private final PerCacheInboundInvocationHandler delegate;
      private final CheckPoint checkPoint;
      protected final AtomicBoolean allowAll = new AtomicBoolean(false);

      private CommandCheckpoint(PerCacheInboundInvocationHandler delegate) {
         this.delegate = delegate;
         this.checkPoint = new CheckPoint();
      }

      protected abstract String tag(T command);

      void handleCommand(T command, Reply reply, DeliverOrder order) {
         String tag = tag(command);
         try {
            if (allowAll.get()) {
               delegate.handle(command, reply, order);
            } else {
               checkPoint.trigger(enterTag(tag));
               checkPoint.awaitStrict(proceedTag(tag), 10, TimeUnit.SECONDS);

               delegate.handle(command, reply, order);

               checkPoint.trigger(doneTag(tag));
               checkPoint.awaitStrict(returnTag(tag), 10, TimeUnit.SECONDS);
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (TimeoutException e) {
            throw new RuntimeException(e);
         }
      }

      protected final void awaitStartTag(String tag) {
         uncheckedAwaitTag(enterTag(tag));
      }

      protected final void awaitDoneTag(String tag) {
         uncheckedAwaitTag(doneTag(tag));
      }

      protected final String enterTag(String tag) {
         return "enter_" + tag;
      }

      protected final String proceedTag(String tag) {
         return "proceed_" + tag;
      }

      protected final String returnTag(String tag) {
         return "return_" + tag;
      }

      protected final String doneTag(String tag) {
         return "done_" + tag;
      }

      protected final void triggerProceedTag(String tag) {
         checkPoint.trigger(proceedTag(tag));
      }

      protected final void triggerProceedTagForever(String tag) {
         checkPoint.triggerForever(proceedTag(tag));
      }

      protected final void triggerReturnTag(String tag) {
         checkPoint.trigger(returnTag(tag));
      }

      protected final void triggerReturnTagForever(String tag) {
         checkPoint.triggerForever(returnTag(tag));
      }

      protected final boolean containsTag(String ... tags) {
         try {
            return checkPoint.peek(-1, TimeUnit.SECONDS, tags) != null;
         } catch (InterruptedException e) {
            return false;
         }
      }

      private void uncheckedAwaitTag(String tag) {
         try {
            checkPoint.awaitStrict(tag, 10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (TimeoutException e) {
            fail("Failed waiting: " + tag);
         }
      }
   }

   public static final class LockCommandCheckPoint extends CommandCheckpoint<LockControlCommand> {

      private LockCommandCheckPoint(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      @Override
      protected String tag(LockControlCommand command) {
         Address address = command.getOrigin();
         GlobalTransaction gtx = command.getGlobalTransaction();
         return lockTag(address, gtx);
      }

      private String lockTag(Address address, GlobalTransaction gtx) {
         return String.format("lock_%s_%s", address, gtx.globalId());
      }

      public void allowLockCommand(Address address, GlobalTransaction gtx) {
         String tag = lockTag(address, gtx);
         triggerProceedTagForever(tag);
         triggerReturnTagForever(tag);
         allowAll.set(true);
      }

      public void awaitStartLockCommand(Address address, GlobalTransaction gtx) {
         String tag = lockTag(address, gtx);
         awaitStartTag(tag);
      }

      public void awaitDoneLockCommand(Address address, GlobalTransaction gtx) {
         String tag = lockTag(address, gtx);
         awaitDoneTag(tag);
      }

      public void proceedLockCommand(Address address, GlobalTransaction gtx) {
         String tag = lockTag(address, gtx);
         triggerProceedTag(tag);
      }

      public void returnLockCommand(Address address, GlobalTransaction gtx) {
         String tag = lockTag(address, gtx);
         triggerReturnTag(tag);
      }
   }

   public static final class DeadlockCommandCheckpoint extends CommandCheckpoint<DeadlockProbeCommand> {

      private DeadlockCommandCheckpoint(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      @Override
      protected String tag(DeadlockProbeCommand command) {
         long initiator = command.getInitiator().getId();
         long holder = command.getGlobalTransaction().getId();
         return tag(initiator, holder);
      }

      private String tag(GlobalTransaction initiator, GlobalTransaction holder) {
         long i = initiator.getId();
         long h = holder.getId();
         return tag(i, h);
      }

      private String tag(long initiator, long holder) {
         return String.format("deadlock_%d_%d", initiator, holder);
      }

      public void allowDeadlockCommand(GlobalTransaction initiator, GlobalTransaction holder) {
         String tag = tag(initiator, holder);
         triggerProceedTagForever(tag);
         triggerReturnTagForever(tag);
      }

      public void awaitStartDeadlockCommand(GlobalTransaction initiator, GlobalTransaction holder) {
         String tag = tag(initiator, holder);
         awaitStartTag(tag);
      }

      public boolean hasAnyOf(GlobalTransaction initiator, GlobalTransaction holder) {
         String tag = tag(initiator, holder);
         return containsTag(enterTag(tag));
      }
   }

   private static Future<DeadlockOperation> runTransaction(TestLeech test, int from, Map<String, String> data, CompletableFuture<GlobalTransaction> ref) {
      return test.fork(() -> {
         AdvancedCache<String, String> cache = test.<String, String>cache(from).getAdvancedCache();
         TransactionTable table = TestingUtil.getTransactionTable(cache);
         TransactionManager tm = cache.getTransactionManager();
         tm.begin();
         CompletableFuture<Void> op = cache.putAllAsync(data);
         test.eventually(() -> table.getGlobalTransaction(tm.getTransaction()) != null);
         ref.complete(table.getGlobalTransaction(tm.getTransaction()));
         try {
            op.get(30, TimeUnit.SECONDS);
            tm.commit();
         } catch (RollbackException e) {
            return new DeadlockOperation(e);
         } catch (Exception e) {
            // Rollback *MUST* be invoked to release the remote locks.
            // Otherwise, the locks are kept in place forever, and we never identify a deadlock.
            tm.rollback();
            return new DeadlockOperation(e);
         }
         return new DeadlockOperation(null);
      });
   }

   private static boolean isDone(int status) {
      return switch (status) {
         case Status.STATUS_PREPARING, Status.STATUS_PREPARED, Status.STATUS_COMMITTING, Status.STATUS_COMMITTED,
              Status.STATUS_ROLLING_BACK, Status.STATUS_ROLLEDBACK, Status.STATUS_UNKNOWN -> true;
         default -> false;
      };
   }

   private static String getOwnedKey(int i, Map<String, String> data) {
      for (String s : data.keySet()) {
         if (s.startsWith(Integer.toString(i)))
            return s;
      }

      throw new IllegalStateException("Not found key owned by: " + i);
   }

   private static Map<String, String> createData(TestLeech test, int... owners) {
      Map<String, String> data = new HashMap<>();
      for (int owner : owners) {
         String key = test.keyGenerator(Integer.toString(owner), owner);
         data.put(key, "some-value");
      }

      return data;
   }

   public static final class DeadlockOperation {
      private final Throwable t;

      public DeadlockOperation(Throwable t) {
         this.t = t;
      }

      public boolean isSuccess() {
         return t == null;
      }

      public Throwable throwable() {
         return t;
      }
   }
}
