package org.infinispan.remoting.transport;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Mock implementation of {@link Transport} that allows intercepting remote calls and replying asynchronously.
 * <p>
 * TODO Allow blocking invocations until the test explicitly unblocks them
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class MockTransport implements Transport {
   private static final Log log = LogFactory.getLog(MockTransport.class);

   private final Address localAddress;
   private final BlockingQueue<FutureRemoteInvocation> expectedInvocations = new LinkedBlockingDeque<>();
   private final BlockingQueue<RecordedRemoteInvocation> recordedInvocations = new LinkedBlockingDeque<>();

   private int viewId;
   private List<Address> members;
   private CompletableFuture<Void> nextViewFuture;

   public MockTransport(Address localAddress) {
      this.localAddress = localAddress;
   }

   public void init(int viewId, List<Address> members) {
      this.viewId = viewId;
      this.members = members;
      this.nextViewFuture = new CompletableFuture<>();
   }

   public void updateView(int viewId, List<Address> members) {
      this.viewId = viewId;
      this.members = members;

      CompletableFuture<Void> nextViewFuture = this.nextViewFuture;
      this.nextViewFuture = new CompletableFuture<>();
      nextViewFuture.complete(null);
   }

   /**
    * Expect a command to be invoked remotely in the future.
    * <p>
    * The operations on the {@link FutureRemoteInvocation} return value are queued and they run on the
    * actual {@link ResponseCollector} the command is invoked with.
    * The test must still call {@link #verifyRemoteCommand(Class)}, and any exceptions thrown by the checker will be
    * rethrown when calling a {@link RecordedRemoteInvocation} method.
    */
   public <T extends ReplicableCommand> FutureRemoteInvocation expectRemoteCommand(Class<T> expectedCommandClass,
                                                                                   Consumer<T> checker) {
      verifyNoMoreCommands();
      FutureRemoteInvocation collector = new FutureRemoteInvocation(c -> {
         T command = expectedCommandClass.cast(c);
         checker.accept(command);
      });
      expectedInvocations.add(collector);
      return collector;
   }

   /**
    * Expect a command to be invoked remotely in the future.
    * <p>
    *
    * @see #expectRemoteCommand(Class, Consumer)
    */
   public <T extends ReplicableCommand> FutureRemoteInvocation expectRemoteCommand(Class<T> expectedCommandClass) {
      verifyNoMoreCommands();
      FutureRemoteInvocation collector = new FutureRemoteInvocation(expectedCommandClass::cast);
      expectedInvocations.add(collector);
      return collector;
   }

   /**
    * Expect a {@link CacheTopologyControlCommand} to be invoked remotely in the future.
    *
    * @see #expectRemoteCommand(Class, Consumer)
    */
   public FutureRemoteInvocation expectTopologyCommand(CacheTopologyControlCommand.Type type,
                                                       Consumer<CacheTopologyControlCommand> checker) {
      verifyNoMoreCommands();
      FutureRemoteInvocation collector = new FutureRemoteInvocation(c -> {
         CacheTopologyControlCommand topologyCommand = (CacheTopologyControlCommand) c;
         assertEquals(type, topologyCommand.getType());
         checker.accept(topologyCommand);
      });
      expectedInvocations.add(collector);
      return collector;
   }

   /**
    * Expect a {@link CacheTopologyControlCommand} to be invoked remotely in the future.
    *
    * @see #expectRemoteCommand(Class, Consumer)
    */
   public FutureRemoteInvocation expectTopologyCommand(CacheTopologyControlCommand.Type type) {
      verifyNoMoreCommands();
      FutureRemoteInvocation collector = new FutureRemoteInvocation(c -> {
         CacheTopologyControlCommand topologyCommand = (CacheTopologyControlCommand) c;
         assertEquals(type, topologyCommand.getType());
      });
      expectedInvocations.add(collector);
      return collector;
   }

   /**
    * Verify that a command was invoked remotely and send replies using the {@link RecordedRemoteInvocation} methods.
    */
   public <T extends ReplicableCommand> RecordedRemoteInvocation verifyRemoteCommand(Class<T> expectedCommandClass,
                                                                                     Consumer<T> checker)
      throws InterruptedException {
      RecordedRemoteInvocation invocation = recordedInvocations.poll(10, TimeUnit.SECONDS);
      assertNotNull("Timed out waiting for invocation", invocation);
      T command = expectedCommandClass.cast(invocation.getCommand());
      checker.accept(command);
      return invocation;
   }

   /**
    * Verify that a command was invoked remotely and send replies using the {@link RecordedRemoteInvocation} methods.
    */
   public <T extends ReplicableCommand> RecordedRemoteInvocation verifyRemoteCommand(Class<T> expectedCommandClass)
      throws InterruptedException {
      return verifyRemoteCommand(expectedCommandClass, c -> {});
   }

   /**
    * Verify that a command was invoked remotely and send replies using the {@link RecordedRemoteInvocation} methods.
    */
   public RecordedRemoteInvocation verifyRemoteCommand() throws InterruptedException {
      return verifyRemoteCommand(ReplicableCommand.class);
   }

   /**
    * Verify that a command was invoked remotely and send replies using the {@link RecordedRemoteInvocation} methods.
    */
   public RecordedRemoteInvocation verifyTopologyCommand(CacheTopologyControlCommand.Type type)
      throws InterruptedException {
      return verifyTopologyCommand(type, c -> {});
   }

   /**
    * Verify that a command was invoked remotely and send replies using the {@link RecordedRemoteInvocation} methods.
    */
   public RecordedRemoteInvocation verifyTopologyCommand(CacheTopologyControlCommand.Type type,
                                                         Consumer<CacheTopologyControlCommand> checker)
      throws InterruptedException {
      RecordedRemoteInvocation invocation = recordedInvocations.poll(10, TimeUnit.SECONDS);
      assertNotNull("Timed out waiting for invocation", invocation);
      CacheTopologyControlCommand command = (CacheTopologyControlCommand) invocation.getCommand();
      assertEquals(type, command.getType());
      checker.accept(command);
      return invocation;
   }

   /**
    * Assert that all the commands already invoked remotely have been verified.
    */
   public void verifyNoMoreCommands() {
      assertTrue("Unexpected remote invocations: " +
                    recordedInvocations.stream().map(i -> i.getCommand().toString()).collect(Collectors.joining(", ")),
                 recordedInvocations.isEmpty());
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, ResponseFilter responseFilter,
                                                DeliverOrder deliverOrder, boolean anycast)
      throws Exception {
      Collection<Address> targets = recipients != null ? recipients : members;
      if (mode == ResponseMode.ASYNCHRONOUS) {
         addInvocation(rpcCommand, MapResponseCollector.validOnly(targets.size()));
         return Collections.emptyMap();
      } else {
         return addInvocationSync(rpcCommand, targets, shouldIgnoreLeavers(mode));
      }
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpcCommand, ResponseMode
                                                                           mode, long timeout, ResponseFilter
                                                                           responseFilter, DeliverOrder
                                                                           deliverOrder, boolean anycast) {
      Collection<Address> targets = recipients != null ? recipients : members;
      MapResponseCollector collector =
         mode.isSynchronous() ? MapResponseCollector.ignoreLeavers(shouldIgnoreLeavers(mode), targets.size()) : null;
      return addInvocation(rpcCommand, collector);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) {
      addInvocation(rpcCommand, null);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) {
      addInvocation(rpcCommand, null);
   }

   @Override
   public void sendToAll(ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      addInvocation(rpcCommand, null);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long
      timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder, boolean anycast) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long
      timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) {
      throw new UnsupportedOperationException();
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isCoordinator() {
      return localAddress.equals(members.get(0));
   }

   @Override
   public Address getCoordinator() {
      return members.get(0);
   }

   @Override
   public Address getAddress() {
      return localAddress;
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      throw new UnsupportedOperationException();
   }

   @Override
   public List<Address> getMembers() {
      return members;
   }

   @Override
   public boolean isMulticastCapable() {
      return true;
   }

   @Override
   public void start() {

   }

   @Override
   public void stop() {

   }

   @Override
   public int getViewId() {
      return viewId;
   }

   @Override
   public CompletableFuture<Void> withView(int expectedViewId) {
      if (viewId <= expectedViewId) {
         return CompletableFutures.completedNull();
      }

      return nextViewFuture.thenCompose(v -> withView(expectedViewId));
   }

   @Override
   public void waitForView(int viewId) throws InterruptedException {
      try {
         withView(viewId).get();
      } catch (ExecutionException e) {
         throw new AssertionError(e);
      }
   }

   @Override
   public Log getLog() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void checkTotalOrderSupported() {
   }

   @Override
   public Set<String> getSitesView() {
      return null;
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command, ResponseCollector<T>
      collector, DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      return addInvocation(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder, long
                                                  timeout, TimeUnit unit) {
      return addInvocation(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      return addInvocation(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                        long timeout, TimeUnit unit) {
      return addInvocation(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets, Function<Address, ReplicableCommand>
      commandGenerator, ResponseCollector<T> responseCollector, DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      throw new UnsupportedOperationException();
   }

   private <T> CompletableFuture<T> addInvocation(ReplicableCommand command, ResponseCollector<T> collector) {
      FutureRemoteInvocation expectedInvocation = expectedInvocations.poll();
      if (expectedInvocation != null) {
         log.debugf("Intercepted expected command %s", command);
         try {
            RecordedRemoteInvocation invocation = new CompletedRemoteInvocation(command);
            recordedInvocations.add(invocation);
            return expectedInvocation.apply(command, collector);
         } catch (Throwable t) {
            recordedInvocations.add(new RemoteInvocationException(t));
            throw t;
         }
      } else {
         log.debugf("Intercepted command %s", command);
         RecordedRemoteInvocation remoteInvocation = new RecordedRemoteInvocation(command, collector);
         recordedInvocations.add(remoteInvocation);
         return remoteInvocation.getResultFuture();
      }
   }

   private Map<Address, Response> addInvocationSync(ReplicableCommand command, Collection<Address> targets, boolean
      ignoreLeavers)
      throws Exception {
      try {
         return addInvocation(command, MapResponseCollector.ignoreLeavers(ignoreLeavers, targets.size()))
            .get(10, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
         throw Util.rewrapAsCacheException(e.getCause());
      }
   }

   private boolean shouldIgnoreLeavers(ResponseMode mode) {
      return mode == ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS;
   }

   /**
    * Mock responses for a remote invocation.
    * <p>
    * For example, {@code remoteInvocation.addResponse(a1, r1).addResponse(a2, r2).finish()},
    * or {@code remoteInvocation.singleResponse(a, r)}
    */
   private static abstract class RemoteInvocation<T extends RemoteInvocation> {
      private final CompletableFuture<Object> resultFuture = new CompletableFuture<>();

      public abstract T addResponse(Address sender, Response response);

      public T addLeaver(Address a) {
         return addResponse(a, CacheNotFoundResponse.INSTANCE);
      }

      public T addException(Address a, Exception e) {
         return addResponse(a, new ExceptionResponse(e));
      }

      public abstract void finish();

      public void singleResponse(Address sender, Response response) {
         addResponse(sender, response);
         if (!isDone()) {
            finish();
         }
      }

      public boolean isDone() {
         return resultFuture.isDone();
      }

      public void complete(Object result) {
         resultFuture.complete(result);
      }

      public void assertDone() {
         assertTrue(resultFuture.isDone());
      }

      @SuppressWarnings("unchecked")
      <U> CompletableFuture<U> getResultFuture() {
         return (CompletableFuture<U>) resultFuture;
      }
   }

   public static class RecordedRemoteInvocation extends RemoteInvocation<RecordedRemoteInvocation> {
      private final ReplicableCommand command;
      private final ResponseCollector<?> collector;

      private RecordedRemoteInvocation(ReplicableCommand command, ResponseCollector collector) {
         this.command = command;
         this.collector = collector;
      }

      @Override
      public RecordedRemoteInvocation addResponse(Address sender, Response response) {
         assertFalse(isDone());

         log.debugf("Replying to remote invocation %s with %s from %s", command, response, sender);
         Object result = collector.addResponse(sender, response);
         if (result != null) {
            complete(result);
         }
         return this;
      }

      @Override
      public void finish() {
         Object result = collector.finish();
         complete(result);
      }

      public ReplicableCommand getCommand() {
         return command;
      }
   }

   private static class CompletedRemoteInvocation extends RecordedRemoteInvocation {
      private CompletedRemoteInvocation(ReplicableCommand command) {
         super(command, null);
         complete(null);
      }

      @Override
      public CompletedRemoteInvocation addResponse(Address sender, Response response) {
         throw new UnsupportedOperationException("No responses expected");
      }

      @Override
      public void finish() {
         throw new UnsupportedOperationException("No responses expected");
      }
   }

   private static class RemoteInvocationException extends RecordedRemoteInvocation {
      private final RuntimeException exception;

      private RemoteInvocationException(Throwable throwable) {
         super(null, null);
         this.exception = new RuntimeException(throwable);
      }

      @Override
      public RemoteInvocationException addResponse(Address sender, Response response) {
         throw exception;
      }

      @Override
      public void finish() {
         throw exception;
      }

      @Override
      public void assertDone() {
         throw exception;
      }
   }

   public static class FutureRemoteInvocation extends RemoteInvocation<FutureRemoteInvocation> {
      private final Consumer<ReplicableCommand> checker;
      private final CompletableFuture<ReplicableCommand> commandFuture;
      private final CompletableFuture<ResponseCollector<?>> collectorFuture;
      private CompletableFuture<Object> operationsFuture;

      private FutureRemoteInvocation(Consumer<ReplicableCommand> checker) {
         this.checker = checker;
         this.collectorFuture = new CompletableFuture<>();
         this.commandFuture = new CompletableFuture<>();
         this.operationsFuture = collectorFuture.thenApply(r -> null);
      }

      @Override
      public FutureRemoteInvocation addResponse(Address sender, Response response) {
         operationsFuture = operationsFuture.thenApply(result -> {
            assertFalse(isDone());
            assertNull(result);

            // join() won't block
            log.debugf("Replying to remote invocation %s with %s from %s", commandFuture.join(), response, sender);
            result = collectorFuture.join().addResponse(sender, response);
            if (result != null) {
               complete(result);
            }
            return result;
         });
         return this;
      }

      @Override
      public void finish() {
         operationsFuture = operationsFuture.thenApply(result -> {
            assertFalse(isDone());
            assertNull(result);

            // join() won't block
            result = collectorFuture.join().finish();
            complete(result);
            return result;
         });
      }

      @SuppressWarnings("unchecked")
      private <T> CompletableFuture<T> apply(ReplicableCommand command, ResponseCollector<T> responseCollector) {
         checker.accept(command);
         commandFuture.complete(command);
         collectorFuture.complete(responseCollector);
         return (CompletableFuture<T>) operationsFuture;
      }
   }
}
