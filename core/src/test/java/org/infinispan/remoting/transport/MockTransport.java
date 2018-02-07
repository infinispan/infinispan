package org.infinispan.remoting.transport;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
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
import org.infinispan.topology.HeartBeatCommand;
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
   private final BlockingQueue<BlockedRequest> blockedRequests = new LinkedBlockingDeque<>();

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
    * Expect a command to be invoked remotely and send replies using the {@link BlockedRequest} methods.
    */
   public <T extends ReplicableCommand> BlockedRequest expectCommand(Class<T> expectedCommandClass)
      throws InterruptedException {
      return expectCommand(expectedCommandClass, c -> {});
   }

   /**
    * Expect a command to be invoked remotely and send replies using the {@link BlockedRequest} methods.
    */
   public <T extends ReplicableCommand> BlockedRequest expectCommand(Class<T> expectedCommandClass,
                                                                     Consumer<T> checker)
      throws InterruptedException {
      BlockedRequest request = blockedRequests.poll(10, TimeUnit.SECONDS);
      assertNotNull("Timed out waiting for invocation", request);
      T command = expectedCommandClass.cast(request.getCommand());
      checker.accept(command);
      return request;
   }

   /**
    * Expect a topology command to be invoked remotely and send replies using the {@link BlockedRequest} methods.
    */
   public BlockedRequest expectTopologyCommand(CacheTopologyControlCommand.Type type)
      throws InterruptedException {
      return expectTopologyCommand(type, c -> {});
   }

   public BlockedRequest expectHeartBeatCommand() throws InterruptedException {
      return expectCommand(HeartBeatCommand.class);
   }

   /**
    * Expect a topology command to be invoked remotely and send replies using the {@link BlockedRequest} methods.
    */
   public BlockedRequest expectTopologyCommand(CacheTopologyControlCommand.Type type,
                                               Consumer<CacheTopologyControlCommand> checker)
      throws InterruptedException {
      return expectCommand(CacheTopologyControlCommand.class, c -> {
         assertEquals(type, c.getType());
         checker.accept(c);
      });
   }

   /**
    * Assert that all the commands already invoked remotely have been verified and there were no errors.
    */
   public void verifyNoErrors() {
      assertTrue("Unexpected remote invocations: " +
                    blockedRequests.stream().map(i -> i.getCommand().toString()).collect(Collectors.joining(", ")),
                 blockedRequests.isEmpty());
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, ResponseFilter responseFilter,
                                                DeliverOrder deliverOrder, boolean anycast) throws Exception {
      Collection<Address> targets = recipients != null ? recipients : members;
      MapResponseCollector collector = MapResponseCollector.ignoreLeavers(shouldIgnoreLeavers(mode), targets.size());
      CompletableFuture<Map<Address, Response>> rpcFuture = blockRequest(rpcCommand, collector);
      if (mode.isAsynchronous()) {
         return Collections.emptyMap();
      } else {
         try {
            return rpcFuture.get(10, TimeUnit.SECONDS);
         } catch (ExecutionException e) {
            throw Util.rewrapAsCacheException(e.getCause());
         }
      }
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpcCommand, ResponseMode mode,
                                                                        long timeout, ResponseFilter responseFilter,
                                                                        DeliverOrder deliverOrder, boolean anycast) {
      Collection<Address> targets = recipients != null ? recipients : members;
      MapResponseCollector collector =
         mode.isSynchronous() ? MapResponseCollector.ignoreLeavers(shouldIgnoreLeavers(mode), targets.size()) : null;
      return blockRequest(rpcCommand, collector);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) {
      blockRequest(rpcCommand, null);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) {
      blockRequest(rpcCommand, null);
   }

   @Override
   public void sendToAll(ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      blockRequest(rpcCommand, null);
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
      return blockRequest(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder, long
                                                  timeout, TimeUnit unit) {
      return blockRequest(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      return blockRequest(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                        long timeout, TimeUnit unit) {
      return blockRequest(command, collector);
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets, Function<Address, ReplicableCommand>
      commandGenerator, ResponseCollector<T> responseCollector, DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      throw new UnsupportedOperationException();
   }

   private <T> CompletableFuture<T> blockRequest(ReplicableCommand command, ResponseCollector<T> collector) {
      log.debugf("Intercepted command %s", command);
      BlockedRequest request = new BlockedRequest(command, collector);
      blockedRequests.add(request);
      return request.getResultFuture();
   }

   private boolean shouldIgnoreLeavers(ResponseMode mode) {
      return mode != ResponseMode.SYNCHRONOUS;
   }

   /**
    * Receive responses for a blocked remote invocation.
    * <p>
    * For example, {@code remoteInvocation.addResponse(a1, r1).addResponse(a2, r2).finish()},
    * or {@code remoteInvocation.singleResponse(a, r)}
    */
   public static class BlockedRequest {
      private final ReplicableCommand command;
      private final ResponseCollector<?> collector;
      private final CompletableFuture<Object> resultFuture = new CompletableFuture<>();

      private BlockedRequest(
         ReplicableCommand command, ResponseCollector collector) {
         this.command = command;
         this.collector = collector;
      }

      public BlockedRequest addResponse(Address sender, Response response) {
         assertFalse(isDone());

         log.debugf("Replying to remote invocation %s with %s from %s", getCommand(), response, sender);
         Object result = collector.addResponse(sender, response);
         if (result != null) {
            complete(result);
         }
         return this;
      }

      public BlockedRequest addLeaver(Address a) {
         return addResponse(a, CacheNotFoundResponse.INSTANCE);
      }

      public BlockedRequest addException(Address a, Exception e) {
         return addResponse(a, new ExceptionResponse(e));
      }

      public void finish() {
         Object result = collector.finish();
         complete(result);
      }

      public void singleResponse(Address sender, Response response) {
         addResponse(sender, response);
         if (!isDone()) {
            finish();
         }
      }

      public ReplicableCommand getCommand() {
         return command;
      }

      boolean isDone() {
         return resultFuture.isDone();
      }

      void complete(Object result) {
         resultFuture.complete(result);
      }

      @SuppressWarnings("unchecked")
      <U> CompletableFuture<U> getResultFuture() {
         return (CompletableFuture<U>) resultFuture;
      }
   }
}
