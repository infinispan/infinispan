package org.infinispan.util;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 4.2
 */
@Scope(Scopes.NAMED_CACHE)
public class ControlledRpcManager extends AbstractDelegatingRpcManager {
   private static final Log log = LogFactory.getLog(ControlledRpcManager.class);
   private static final int TIMEOUT_SECONDS = 10;

   @Inject Cache<?, ?> cache;
   @Inject @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutExecutor;
   @Inject @ComponentName(NON_BLOCKING_EXECUTOR)
   ExecutorService nonBlockingExecutor;

   private volatile boolean stopped = false;
   private final Set<Class<? extends ReplicableCommand>> excludedCommands =
      Collections.synchronizedSet(new HashSet<>());
   private final BlockingQueue<CompletableFuture<ControlledRequest<?>>> waiters = new LinkedBlockingDeque<>();
   private RuntimeException globalError;

   protected ControlledRpcManager(RpcManager realOne, Cache<?, ?> cache) {
      super(realOne);
      this.cache = cache;
   }

   public static ControlledRpcManager replaceRpcManager(Cache<?, ?> cache) {
      RpcManager rpcManager = extractComponent(cache, RpcManager.class);
      if (rpcManager instanceof ControlledRpcManager) {
         throw new IllegalStateException("One ControlledRpcManager per cache should be enough");
      }
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(rpcManager, cache);
      log.tracef("Installing ControlledRpcManager on %s", controlledRpcManager.getAddress());
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }

   public void revertRpcManager() {
      stopBlocking();
      log.tracef("Restoring regular RpcManager on %s", getAddress());
      RpcManager rpcManager = extractComponent(cache, RpcManager.class);
      assertSame(this, rpcManager);
      TestingUtil.replaceComponent(cache, RpcManager.class, realOne, true);
   }

   @SafeVarargs
   public final void excludeCommands(Class<? extends ReplicableCommand>... excluded) {
      if (stopped) {
         throw new IllegalStateException("Trying to exclude commands but we already stopped intercepting");
      }
      excludedCommands.clear();
      excludedCommands.addAll(Arrays.asList(excluded));
   }

   public void stopBlocking() {
      log.debugf("Stopping intercepting RPC calls on %s", realOne.getAddress());
      stopped = true;
      throwGlobalError();
      if (!waiters.isEmpty()) {
         fail("Stopped intercepting RPCs on " + realOne.getAddress() + ", but there are " + waiters.size() + " waiters in the queue");
      }
   }

   /**
    * Expect a command to be invoked remotely and send replies using the {@link BlockedRequest} methods.
    */
   public <T extends ReplicableCommand> BlockedRequest<T> expectCommand(Class<T> expectedCommandClass) {
      return uncheckedGet(expectCommandAsync(expectedCommandClass));
   }

   /**
    * Expect a command to be invoked remotely and send replies using the {@link BlockedRequest} methods.
    */
   public <T extends ReplicableCommand>
   BlockedRequest<T> expectCommand(Class<T> expectedCommandClass, Consumer<T> checker) {
      BlockedRequest<T> blockedRequest = uncheckedGet(expectCommandAsync(expectedCommandClass));
      T command = expectedCommandClass.cast(blockedRequest.request.getCommand());
      checker.accept(command);
      return blockedRequest;
   }

   public <T extends ReplicableCommand>
   BlockedRequests<T> expectCommands(Class<T> expectedCommandClass, Address... targets) {
      return expectCommands(expectedCommandClass, Arrays.asList(targets));
   }

   public <T extends ReplicableCommand>
   BlockedRequests<T> expectCommands(Class<T> expectedCommandClass, Collection<Address> targets) {
      Map<Address, BlockedRequest<T>> requests = new HashMap<>();
      for (int i = 0; i < targets.size(); i++) {
         BlockedRequest<T> request = expectCommand(expectedCommandClass);
         requests.put(request.getTarget(), request);
      }
      assertEquals(new HashSet<>(targets), requests.keySet());
      return new BlockedRequests<>(requests);
   }

   /**
    * Expect a command to be invoked remotely and send replies using the {@link BlockedRequest} methods.
    */
   public <T extends ReplicableCommand>
   CompletableFuture<BlockedRequest<T>> expectCommandAsync(Class<T> expectedCommandClass) {
      throwGlobalError();
      log.tracef("Waiting for command %s", expectedCommandClass);
      CompletableFuture<ControlledRequest<?>> future = new CompletableFuture<>();
      waiters.add(future);
      return future.thenApply(request -> {
         log.tracef("Blocked command %s", request.command);
         assertTrue("Expecting a " + expectedCommandClass.getName() + ", got " + request.getCommand(),
                    expectedCommandClass.isInstance(request.getCommand()));
         return new BlockedRequest<>(request);
      });
   }

   public void expectNoCommand() {
      throwGlobalError();
      assertNull("There should be no queued commands", waiters.poll());
   }

   public void expectNoCommand(long timeout, TimeUnit timeUnit) throws InterruptedException {
      throwGlobalError();
      assertNull("There should be no queued commands", waiters.poll(timeout, timeUnit));
   }

   @Override
   protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                   ResponseCollector<T> collector,
                                                   Function<ResponseCollector<T>, CompletionStage<T>> invoker,
                                                   RpcOptions rpcOptions) {
      if (stopped || isCommandExcluded(command)) {
         log.tracef("Not blocking excluded command %s", command);
         return invoker.apply(collector);
      }
      log.debugf("Intercepted command to %s: %s", targets, command);
      // Ignore the SingleRpcCommand wrapper
      if (command instanceof SingleRpcCommand) {
         command = ((SingleRpcCommand) command).getCommand();
      }
      Address excluded = realOne.getAddress();
      ControlledRequest<T> controlledRequest =
         new ControlledRequest<>(command, targets, collector, invoker, nonBlockingExecutor, excluded);
      try {
         CompletableFuture<ControlledRequest<?>> waiter = waiters.poll(TIMEOUT_SECONDS, SECONDS);
         if (waiter == null) {
            TimeoutException t = new TimeoutException("Found no waiters for command " + command);
            addGlobalError(t);
            throw t;
         }
         waiter.complete(controlledRequest);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new TestException(e);
      } catch (Exception e) {
         throw new TestException(e);
      }
      if (collector != null) {
         ScheduledFuture<?> cancelTask = timeoutExecutor.schedule(() -> {
            TimeoutException e = new TimeoutException("Timed out waiting for test to unblock command " +
                                                      controlledRequest.getCommand());
            addGlobalError(e);
            controlledRequest.fail(e);
         }, TIMEOUT_SECONDS * 2, SECONDS);
         controlledRequest.resultFuture.whenComplete((ignored, throwable) -> cancelTask.cancel(false));
      }
      // resultFuture is completed from a test thread, and we don't want to run the interceptor callbacks there
      return controlledRequest.resultFuture.whenCompleteAsync((r, t) -> {}, nonBlockingExecutor);
   }

   private void addGlobalError(RuntimeException t) {
      if (globalError == null) {
         globalError = t;
      } else {
         globalError.addSuppressed(t);
      }
   }

   @Override
   protected <T> void performSend(Collection<Address> targets, ReplicableCommand command,
                                  Function<ResponseCollector<T>, CompletionStage<T>> invoker) {
      performRequest(targets, command, null, invoker, null);
   }

   @Stop
   void stop() {
      stopBlocking();
      TestingUtil.stopComponent(realOne);
   }

   private boolean isCommandExcluded(ReplicableCommand command) {
      for (Class<? extends ReplicableCommand> excludedCommand : excludedCommands) {
         if (excludedCommand.isInstance(command))
            return true;
      }
      return false;
   }

   private void throwGlobalError() {
      if (globalError != null) {
         throw globalError;
      }
   }

   static <T> T uncheckedGet(CompletionStage<T> stage) {
      try {
         return stage.toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
      } catch (Exception e) {
         throw new TestException(e);
      }
   }

   /**
    * A controlled request.
    *
    * The real RpcManager will not send the command to the targets until the test calls {@link #send()}.
    * Responses received from the targets are stored in {@link #responseFutures}, and after the last response
    * is received they are also stored in the {@link #finishFuture} map.
    *
    * The responses are only passed to the real response collector when the test calls
    * {@link #collectResponse(Address, Response)}, and {@link #collectFinish()} finishes the collector.
    */
   static class ControlledRequest<T> {
      private final ReplicableCommand command;
      private final Collection<Address> targets;
      private final Function<ResponseCollector<T>, CompletionStage<T>> invoker;
      private final ExecutorService executor;

      private final CompletableFuture<T> resultFuture = new CompletableFuture<>();
      private final LinkedHashMap<Address, CompletableFuture<Response>> responseFutures = new LinkedHashMap<>();
      private final CompletableFuture<Map<Address, Response>> finishFuture = new CompletableFuture<>();
      private final CompletableFuture<Void> sendFuture = new CompletableFuture<>();

      private final Lock collectLock = new ReentrantLock();
      @GuardedBy("collectLock")
      private final ResponseCollector<T> collector;
      @GuardedBy("collectLock")
      private final Set<Address> collectedResponses = new HashSet<>();
      @GuardedBy("collectLock")
      private boolean collectedFinish;


      ControlledRequest(ReplicableCommand command, Collection<Address> targets, ResponseCollector<T> collector,
                        Function<ResponseCollector<T>, CompletionStage<T>> invoker,
                        ExecutorService executor, Address excluded) {
         this.command = command;
         this.targets = targets;
         this.collector = collector;
         this.invoker = invoker;
         this.executor = executor;

         for (Address target : targets) {
            if (!target.equals(excluded)) {
               responseFutures.put(target, new CompletableFuture<>());
            }
         }
      }

      void send() {
         invoker.apply(new ResponseCollector<T>() {
            @Override
            public T addResponse(Address sender, Response response) {
               queueResponse(sender, response);
               return null;
            }

            @Override
            public T finish() {
               queueFinish();
               return null;
            }
         });

         sendFuture.complete(null);
      }

      void skipSend() {
         sendFuture.complete(null);
         for (CompletableFuture<Response> responseFuture : responseFutures.values()) {
            responseFuture.complete(null);
         }
      }

      void awaitSend() {
         uncheckedGet(sendFuture);
      }

      private void queueResponse(Address sender, Response response) {
         log.tracef("Queueing response from %s for command %s", sender, command);
         CompletableFuture<Response> responseFuture = responseFutures.get(sender);
         boolean completedNow = responseFuture.complete(response);
         if (!completedNow) {
            fail(new IllegalStateException("Duplicate response received from " + sender + ": " + response));
         }
      }

      private void queueFinish() {
         log.tracef("Queueing finish for command %s", command);
         Map<Address, Response> responseMap = new LinkedHashMap<>();
         for (Map.Entry<Address, CompletableFuture<Response>> entry : responseFutures.entrySet()) {
            Address sender = entry.getKey();
            CompletableFuture<Response> responseFuture = entry.getValue();
            // Don't wait for all responses in case this is a staggered request
            if (responseFuture.isDone()) {
               responseMap.put(sender, uncheckedGet(responseFuture));
            } else {
               responseFuture.complete(null);
            }
         }
         boolean completedNow = finishFuture.complete(responseMap);
         if (!completedNow) {
            fail(new IllegalStateException("Finish queued more than once"));
         }
      }

      void collectResponse(Address sender, Response response) {
         try {
            T result;
            collectLock.lock();
            try {
               throwIfFailed();
               assertTrue(collectedResponses.add(sender));
               result = collector.addResponse(sender, response);
               if (result != null) {
                  // Don't allow collectFinish on this request
                  collectedFinish = true;
               }
            } finally {
               collectLock.unlock();
            }
            if (result != null) {
               resultFuture.complete(result);
            }
         } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
         }
      }

      void collectFinish() {
         try {
            T result;
            collectLock.lock();
            try {
               throwIfFailed();
               assertFalse(collectedFinish);

               collectedFinish = true;
               result = collector.finish();
            } finally {
               collectLock.unlock();
            }
            resultFuture.complete(result);
         } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
         }
      }

      void skipFinish() {
         collectLock.lock();
         try {
            assertFalse(collectedFinish);
         } finally {
            collectLock.unlock();
         }
         assertTrue(resultFuture.isDone());
      }

      void fail(Throwable t) {
         log.tracef("Failing execution of %s with %s", command, t);
         resultFuture.completeExceptionally(t);

         // Unblock the thread waiting for the request to be sent, if it's not already sent
         sendFuture.completeExceptionally(t);
      }

      void throwIfFailed() {
         if (resultFuture.isCompletedExceptionally()) {
            resultFuture.join();
         }
      }

      boolean isDone() {
         return resultFuture.isDone();
      }

      ReplicableCommand getCommand() {
         return command;
      }

      Collection<Address> getTargets() {
         return targets;
      }

      boolean hasCollector() {
         return collector != null;
      }

      CompletableFuture<Response> responseFuture(Address sender) {
         return responseFutures.get(sender);
      }

      CompletableFuture<Map<Address, Response>> finishFuture() {
         return finishFuture;
      }
   }

   /**
    * Unblock and wait for the responses of a blocked remote invocation.
    * <p>
    * For example, {@code request.send().expectResponse(a1, r1).replace(r2).receiveAll()}.
    */
   public static class BlockedRequest<C extends ReplicableCommand> {
      private final ControlledRequest<?> request;

      public BlockedRequest(ControlledRequest<?> request) {
         this.request = request;
      }

      /**
       * Unblock the request, sending it to its targets.
       * <p>
       * It will block again when waiting for responses.
       */
      public SentRequest send() {
         log.tracef("Sending command %s", request.getCommand());
         request.send();

         if (request.hasCollector()) {
            return new SentRequest(request);
         } else {
            return null;
         }
      }

      /**
       * Avoid sending the request, and finish it with the given responses instead.
       */
      public FakeResponses skipSend() {
         log.tracef("Not sending request %s", request.getCommand());
         request.skipSend();

         if (request.hasCollector()) {
            return new FakeResponses(request);
         } else {
            return null;
         }
      }

      public void fail() {
         fail(new TestException("Induced failure!"));
      }

      public void fail(Exception e) {
         request.fail(e);
      }

      public C getCommand() {
         return (C) request.getCommand();
      }

      public Collection<Address> getTargets() {
         return request.getTargets();
      }

      public Address getTarget() {
         Collection<Address> targets = request.getTargets();
         assertEquals(1, targets.size());
         return targets.iterator().next();
      }
   }

   public static class SentRequest {
      private final ControlledRequest<?> request;

      SentRequest(ControlledRequest<?> request) {
         this.request = request;
      }

      /**
       * Complete the request with a {@link TimeoutException}
       */
      public void forceTimeout() {
         assertFalse(request.isDone());
         request.fail(log.requestTimedOut(-1, "Induced timeout failure", "some time"));
      }

      /**
       * Wait for a response from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectResponse(Address sender, Consumer<Response> checker) {
         BlockedResponse br = uncheckedGet(expectResponseAsync(sender));
         checker.accept(br.response);
         return br;
      }

      /**
       * Wait for a response from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectResponse(Address sender) {
         return uncheckedGet(expectResponseAsync(sender));
      }

      /**
       * Wait for a response from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectResponse(Address sender, Response expectedResponse) {
         return expectResponse(sender, r -> assertEquals(expectedResponse, r));
      }

      /**
       * Wait for a {@code CacheNotFoundResponse} from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectLeaver(Address a) {
         return expectResponse(a, CacheNotFoundResponse.INSTANCE);
      }

      /**
       * Wait for an {@code ExceptionResponse} from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectException(Address a, Class<? extends Exception> expectedException) {
         return expectResponse(a, r -> {
            Exception exception = ((ExceptionResponse) r).getException();
            Exceptions.assertException(expectedException, exception);
         });
      }

      /**
       * Wait for all the responses.
       */
      public BlockedResponseMap expectAllResponses() {
         return uncheckedGet(expectAllResponsesAsync());
      }

      /**
       * Wait for all the responses.
       */
      public BlockedResponseMap expectAllResponses(BiConsumer<Address, Response> checker) {
         BlockedResponseMap blockedResponseMap = uncheckedGet(expectAllResponsesAsync());
         blockedResponseMap.responseMap.forEach(checker);
         return blockedResponseMap;
      }

      /**
       * Wait for all the responses and process them.
       */
      public void receiveAll() {
         expectAllResponses().receive();
      }

      public void receiveAllAsync() {
         expectAllResponsesAsync().thenAccept(BlockedResponseMap::receive);
      }

      /**
       * Complete a request after expecting and receiving responses individually, e.g. with
       * {@link #expectResponse(Address)}.
       *
       * This method blocks until all the responses have been received internally, but doesn't pass them on
       * to the original response collector (it only calls {@link ResponseCollector#finish()}).
       */
      public void finish() {
         uncheckedGet(request.finishFuture());
         request.collectFinish();
      }

      public void noFinish() {
         request.skipFinish();
      }

      public CompletionStage<BlockedResponse> expectResponseAsync(Address sender) {
         request.throwIfFailed();
         assertFalse(request.isDone());

         return request.responseFuture(sender).thenApply(response -> {
            log.debugf("Got response for %s from %s: %s", request.getCommand(), sender, response);
            return new BlockedResponse(request, this, sender, response);
         });
      }

      public CompletionStage<BlockedResponseMap> expectAllResponsesAsync() {
         request.throwIfFailed();
         assertFalse(request.isDone());

         return request.finishFuture()
                       .thenApply(responseMap -> new BlockedResponseMap(request, responseMap));
      }
   }

   public static class BlockedResponse {
      private final ControlledRequest<?> request;
      final SentRequest sentRequest;
      final Address sender;
      final Response response;

      private BlockedResponse(ControlledRequest<?> request, SentRequest sentRequest, Address sender,
                              Response response) {
         this.request = request;
         this.sentRequest = sentRequest;
         this.sender = sender;
         this.response = response;
      }

      /**
       * Process the response from this {@code BlockedResponse}'s target.
       * <p>
       * Note that processing the last response will NOT complete the request, you still need to call
       * {@link SentRequest#receiveAll()}.
       */
      public SentRequest receive() {
         log.tracef("Unblocking response from %s: %s", sender, response);
         request.collectResponse(this.sender, response);
         return sentRequest;
      }

      /**
       * Replace the response from this {@code BlockedResponse}'s target with a fake response and process it.
       */
      public SentRequest replace(Response newResponse) {
         log.tracef("Replacing response from %s: %s (was %s)", sender, newResponse, response);
         request.collectResponse(this.sender, newResponse);
         return sentRequest;
      }

      public CompletionStage<SentRequest> receiveAsync() {
         return CompletableFuture.supplyAsync(this::receive, request.executor);
      }

      public CompletionStage<SentRequest> replaceAsync(Response newResponse) {
         return CompletableFuture.supplyAsync(() -> replace(newResponse), request.executor);
      }

      public Address getSender() {
         return sender;
      }

      public Response getResponse() {
         return response;
      }
   }

   public static class BlockedResponseMap {
      private final ControlledRequest<?> request;
      private final Map<Address, Response> responseMap;

      private BlockedResponseMap(ControlledRequest<?> request,
                                 Map<Address, Response> responseMap) {
         this.request = request;
         this.responseMap = responseMap;
      }

      public void receive() {
         assertFalse(request.resultFuture.isDone());

         log.tracef("Unblocking responses for %s: %s", request.getCommand(), responseMap);
         responseMap.forEach(request::collectResponse);
         if (!request.isDone()) {
            uncheckedGet(request.finishFuture());
            request.collectFinish();
         }
      }

      public void replace(Map<Address, Response> newResponses) {
         assertFalse(request.resultFuture.isDone());

         log.tracef("Replacing responses for %s: %s (was %s)", request.getCommand(), newResponses, responseMap);
         newResponses.forEach(request::collectResponse);
         if (!request.isDone()) {
            uncheckedGet(request.finishFuture());
            request.collectFinish();
         }
      }

      public CompletionStage<Void> receiveAsync() {
         return CompletableFuture.runAsync(this::receive, request.executor);
      }

      public CompletionStage<Void> replaceAsync(Map<Address, Response> newResponses) {
         return CompletableFuture.runAsync(() -> replace(newResponses), request.executor);
      }

      public Map<Address, Response> getResponses() {
         return responseMap;
      }
   }

   public static class FakeResponses {
      private final ControlledRequest<?> request;

      public FakeResponses(ControlledRequest<?> request) {
         this.request = request;
      }

      public void receive(Map<Address, Response> responses) {
         log.tracef("Faking responses for %s: %s", request.getCommand(), responses);
         responses.forEach((sender, response) -> {
            // For staggered requests we allow the test to specify only the primary owner's response
            assertTrue(responses.containsKey(sender));
            request.collectResponse(sender, response);
         });
         if (!request.isDone()) {
            assertEquals(responses.keySet(), request.responseFutures.keySet());
            request.collectFinish();
         }
      }

      public void receive(Address sender, Response response) {
         receive(Collections.singletonMap(sender, response));
      }

      public void receive(Address sender1, Response response1,
                          Address sender2, Response response2) {
         Map<Address, Response> responses = new LinkedHashMap<>();
         responses.put(sender1, response1);
         responses.put(sender2, response2);
         receive(responses);
      }

      public void receive(Address sender1, Response response1,
                          Address sender2, Response response2,
                          Address sender3, Response response3) {
         Map<Address, Response> responses = new LinkedHashMap<>();
         responses.put(sender1, response1);
         responses.put(sender2, response2);
         responses.put(sender3, response3);
         receive(responses);
      }

      public CompletionStage<Void> receiveAsync(Map<Address, Response> responses) {
         return CompletableFuture.runAsync(() -> receive(responses), request.executor);
      }

      public CompletionStage<Void> receiveAsync(Address sender, Response response) {
         return CompletableFuture.runAsync(() -> receive(sender, response), request.executor);
      }

      public CompletionStage<Void> receiveAsync(Address sender1, Response response1,
                                                Address sender2, Response response2) {
         return CompletableFuture.runAsync(() -> receive(sender1, response1, sender2, response2), request.executor);
      }

      /**
       * Complete the request with a {@link TimeoutException}
       */
      public void forceTimeout() {
         fail(log.requestTimedOut(-1, "Induced failure", "some time"));
      }

      /**
       * Complete the request with a custom exception.
       */
      private void fail(Throwable e) {
         assertFalse(request.resultFuture.isDone());
         request.fail(e);
      }

      public Collection<Address> getTargets() {
         return request.getTargets();
      }

      public Address getTarget() {
         Collection<Address> targets = request.getTargets();
         assertEquals(1, targets.size());
         return targets.iterator().next();
      }
   }

   /**
    * Multiple requests sent to individual targets in parallel, e.g. with
    * {@link RpcManager#invokeCommands(Collection, Function, ResponseCollector, RpcOptions)}.
    */
   public static class BlockedRequests<T extends ReplicableCommand> {
      private final Map<Address, BlockedRequest<T>> requests;

      public BlockedRequests(Map<Address, BlockedRequest<T>> requests) {
         this.requests = requests;
      }

      /**
       * Unblock the request, sending it to its targets.
       * <p>
       * It will block again when waiting for responses.
       */
      public SentRequest send(Address target) {
         return requests.get(target).send();
      }

      /**
       * Avoid sending the request, and finish it with the given responses instead.
       */
      public FakeResponses skipSend(Address target) {
         return requests.get(target).skipSend();
      }

      public void skipSendAndReceive(Address target, Response fakeResponse) {
         requests.get(target).skipSend().receive(target, fakeResponse);
      }

      public void skipSendAndReceiveAsync(Address target, Response fakeResponse) {
         requests.get(target).skipSend().receiveAsync(target, fakeResponse);
      }
   }
}
