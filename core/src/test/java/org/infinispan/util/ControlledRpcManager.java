package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.jcip.annotations.GuardedBy;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 4.2
 */
public class ControlledRpcManager extends AbstractDelegatingRpcManager {
   private static final Log log = LogFactory.getLog(ControlledRpcManager.class);
   private static final int TIMEOUT_SECONDS = 10;

   private final AtomicInteger count = new AtomicInteger(1);
   private final Cache<?, ?> cache;

   private volatile boolean stopped = false;
   private final Set<Class<? extends ReplicableCommand>> excludedCommands =
      Collections.synchronizedSet(new HashSet<>());
   private final BlockingQueue<InternalRequest> queuedRequests = new LinkedBlockingDeque<>();
   private final ScheduledExecutorService executor;

   protected ControlledRpcManager(RpcManager realOne, Cache<?, ?> cache) {
      super(realOne);
      this.cache = cache;
      executor = Executors.newScheduledThreadPool(
         0, r -> new Thread(r, "ControlledRpc-" + count.getAndIncrement() + "," + realOne.getAddress()));
   }

   public static ControlledRpcManager replaceRpcManager(Cache<?, ?> cache) {
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(rpcManager, cache);
      log.tracef("Installing ControlledRpcManager on %s", controlledRpcManager.getAddress());
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }

   public void revertRpcManager() {
      stopBlocking();
      log.tracef("Restoring regular RpcManager on %s", getAddress());
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
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
      log.debug("Stopping intercepting RPC calls");
      stopped = true;
      executor.shutdownNow();
      if (!queuedRequests.isEmpty()) {
         List<ReplicableCommand> commands = queuedRequests.stream().map(r -> r.command).collect(Collectors.toList());
         fail("Stopped intercepting RPCs, but there are " + queuedRequests.size() + " blocked requests in the queue: " + commands);
      }
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
      log.tracef("Waiting for command %s", expectedCommandClass);
      InternalRequest request = queuedRequests.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      log.tracef("Fetched command %s", request != null ? request.command : null);
      assertNotNull("Timed out waiting for invocation", request);
      assertTrue("Expecting a " + expectedCommandClass.getName() + ", got " + request.getCommand(),
                 expectedCommandClass.isInstance(request.getCommand()));
      T command = expectedCommandClass.cast(request.getCommand());
      checker.accept(command);
      return new BlockedRequest(request);
   }

   public void expectNoCommand() {
      assertNull("There should be no queued commands", queuedRequests.poll());
   }

   public void expectNoCommand(long timeout, TimeUnit timeUnit) throws InterruptedException {
      assertNull("There should be no queued commands", queuedRequests.poll(timeout, timeUnit));
   }

   @Override
   protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                   ResponseCollector<T> collector,
                                                   Function<ResponseCollector<T>, CompletionStage<T>> invoker) {
      if (stopped || commandExcluded(command)) {
         log.tracef("Not blocking excluded command %s", command);
         return invoker.apply(collector);
      }
      log.debugf("Intercepted command %s", command);
      // Ignore the SingleRpcCommand wrapper
      if (command instanceof SingleRpcCommand) {
         command = ((SingleRpcCommand) command).getCommand();
      }
      InternalRequest<T> internalRequest = new InternalRequest<>(command, targets, collector, invoker);
      queuedRequests.add(internalRequest);
      internalRequest.awaitInvoke();
      if (collector != null) {
         executor.schedule(internalRequest::cancel, TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);
      }
      // resultFuture is completed from a test thread, and we don't want to run the interceptor callbacks there
      return internalRequest.resultFuture.whenCompleteAsync((r, t) -> {}, executor);
   }

   @Override
   protected <T> void performSend(Collection<Address> targets, ReplicableCommand command,
                                  Function<ResponseCollector<T>, CompletionStage<T>> invoker) {
      performRequest(targets, command, null, invoker);
   }

   private boolean commandExcluded(ReplicableCommand command) {
      for (Class<? extends ReplicableCommand> excludedCommand : excludedCommands) {
         if (excludedCommand.isInstance(command))
            return true;
      }
      return false;
   }

   static class InternalRequest<T> {
      private final ReplicableCommand command;
      private final Collection<Address> targets;
      private final ResponseCollector<T> collector;
      private final Function<ResponseCollector<T>, CompletionStage<T>> invoker;
      private final CompletableFuture<T> resultFuture = new CompletableFuture<>();

      private final Lock lock = new ReentrantLock();
      private final Condition queueCondition = lock.newCondition();
      @GuardedBy("lock")
      private boolean sent;
      @GuardedBy("lock")
      private final LinkedHashMap<Address, Response> queuedResponses = new LinkedHashMap<>();
      @GuardedBy("lock")
      private boolean queuedFinish;
      @GuardedBy("lock")
      private final LinkedHashMap<Address, Response> collectedResponses = new LinkedHashMap<>();

      InternalRequest(ReplicableCommand command, Collection<Address> targets, ResponseCollector<T> collector,
                      Function<ResponseCollector<T>, CompletionStage<T>> invoker) {
         this.command = command;
         this.targets = targets;
         this.collector = collector;
         this.invoker = invoker;
      }

      void invoke() {
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

         markAsSent();
      }

      void markAsSent() {
         lock.lock();
         try {
            sent = true;
            queueCondition.signalAll();
         } finally {
            lock.unlock();
         }
      }

      void awaitInvoke() {
         lock.lock();
         try {
            long remainingNanos = TimeUnit.NANOSECONDS.convert(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            while (!sent) {
               throwIfFailed();

               remainingNanos = queueCondition.awaitNanos(remainingNanos);
               if (remainingNanos < 0) {
                  fail(new TimeoutException("Timed out waiting for the test to send command " + command));
               }
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TestException(e);
         } finally {
            lock.unlock();
         }
      }

      void queueResponse(Address sender, Response response) {
         lock.lock();
         try {
            Response previous = queuedResponses.put(sender, response);
            if (previous != null) {
               resultFuture.completeExceptionally(new IllegalStateException(
                  "Duplicate response received from " + sender + ": " + response));
            }
            queueCondition.signalAll();
         } finally {
            lock.unlock();
         }
      }

      void queueFinish() {
         lock.lock();
         try {
            if (queuedFinish) {
               resultFuture.completeExceptionally(new IllegalStateException("Duplicate finish"));
            }
            queuedFinish = true;
            queueCondition.signalAll();
         } finally {
            lock.unlock();
         }
      }

      /**
       * Wait for a new response from the given sender.
       */
      Response peekResponse(Address sender) throws InterruptedException {
         lock.lock();
         try {
            long remainingNanos = TimeUnit.NANOSECONDS.convert(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            while (true) {
               throwIfFailed();

               Response response = queuedResponses.get(sender);
               if (response != null) {
                  return response;
               }

               remainingNanos = queueCondition.awaitNanos(remainingNanos);
               if (remainingNanos < 0) {
                  fail(new TimeoutException("Timed out waiting for a response from " + sender + " for " + command));
               }
            }
         } finally {
            lock.unlock();
         }
      }

      /**
       * Wait for a new response.
       *
       * @return either a {@code <sender, response>} pair or {@code null} if all the responses have been collected.
       */
      Map.Entry<Address, Response> peekResponse() throws InterruptedException {
         lock.lock();
         try {
            if (collector == null) {
               throw new IllegalStateException("Cannot wait for responses on sendTo/sendToMany/sendToAll");
            }

            long remainingNanos = TimeUnit.NANOSECONDS.convert(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            while (true) {
               for (Map.Entry<Address, Response> entry : queuedResponses.entrySet()) {
                  if (!collectedResponses.containsKey(entry.getKey()))
                     return new AbstractMap.SimpleImmutableEntry<>(entry);
               }

               if (queuedFinish) {
                  return null;
               }

               remainingNanos = queueCondition.awaitNanos(remainingNanos);
               if (remainingNanos < 0) {
                  TimeoutException e = new TimeoutException("Timed out waiting for a response for " + command);
                  fail(e);
                  throw e;
               }
            }
         } finally {
            lock.unlock();
         }
      }

      /**
       * Wait for the internal finish.
       *
       * @return the responses that haven't been collected yet.
       */
      Map<Address, Response> peekFinish() throws InterruptedException {
         lock.lock();
         try {
            long remainingNanos = TimeUnit.NANOSECONDS.convert(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            while (true) {
               throwIfFailed();

               if (queuedFinish) {
                  LinkedHashMap<Address, Response> responseMap = new LinkedHashMap<>(queuedResponses);
                  responseMap.keySet().removeAll(collectedResponses.keySet());
                  return responseMap;
               }

               remainingNanos = queueCondition.awaitNanos(remainingNanos);
               if (remainingNanos < 0) {
                  fail(new TimeoutException("Timed out waiting for internal finish for " + command));
               }
            }
         } finally {
            lock.unlock();
         }
      }

      void collectResponse(Address sender, Response response) {
         lock.lock();
         try {
            throwIfFailed();

            Response previous = collectedResponses.put(sender, response);
            if (previous != null) {
               throw new AssertionError("Duplicate response received from " + sender + ": " + response);
            }

            if (!resultFuture.isDone()) {
               try {
                  T result = collector.addResponse(sender, response);
                  if (result != null) {
                     resultFuture.complete(result);
                  }
               } catch (Throwable t) {
                  resultFuture.completeExceptionally(t);
               }
            }
         } finally {
            lock.unlock();
         }
      }

      void collectFinish() {
         lock.lock();
         try {
            throwIfFailed();

            if (!queuedFinish) {
               throw new IllegalStateException("Trying to finish the request before all the responses were processed internally");
            }

            if (!resultFuture.isDone()) {
               T result = collector.finish();
               resultFuture.complete(result);
            }
         } finally {
            lock.unlock();
         }
      }

      void fail(Throwable t) {
         log.tracef("Failing execution of %s, currently %s", command, resultFuture);
         lock.lock();
         try {
            throwIfFailed();

            if (resultFuture.isDone()) {
               throw new IllegalStateException("Trying to fail a request after it has already finished");
            }

            // unblock the thread waiting for the request to be sent, not just response
            sent = true;
            queueCondition.signalAll();

            resultFuture.completeExceptionally(t);
         } finally {
            lock.unlock();
            log.tracef("Result future is %s", resultFuture);
         }
      }

      void throwIfFailed() {
         if (resultFuture.isCompletedExceptionally()) {
            resultFuture.join();
         }
      }

      void cancel() {
         if (!resultFuture.isDone()) {
            fail(new TimeoutException("Timed out waiting for test to unblock command " + command));
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
   }

   /**
    * Unblock and wait for the responses of a blocked remote invocation.
    * <p>
    * For example, {@code request.send().expectResponse(a1, r1).replace(r2).receiveAll()}.
    */
   public static class BlockedRequest {
      private final InternalRequest<?> request;

      public BlockedRequest(InternalRequest<?> request) {
         this.request = request;
      }

      /**
       * Unblock the request, sending it to its targets.
       * <p>
       * It will block again when waiting for responses.
       */
      public SentRequest send() {
         assertFalse(request.sent);
         assertNotNull("Please use sendWithoutResponses() when the caller doesn't expect any responses",
                       request.collector);

         log.tracef("Sending request %s", request.getCommand());
         request.invoke();

         return new SentRequest(request);
      }

      public void sendWithoutResponses() {
         assertFalse(request.sent);
         assertNull("Please use send() when the caller does expect responses", request.collector);

         log.tracef("Sending command %s", request.getCommand());
         request.invoke();
      }

      /**
       * Avoid sending the request, and finish it with the given responses instead.
       */
      public FakeResponses skipSend() {
         assertFalse(request.sent);

         request.markAsSent();

         log.tracef("Not sending request %s", request.getCommand());
         return new FakeResponses(request);
      }

      public void fail() {
         fail(new TestException("Induced failure!"));
      }

      public void fail(Exception e) {
         request.fail(e);
      }

      public Collection<Address> getTargets() {
         return request.getTargets();
      }
   }

   public static class SentRequest {
      private final InternalRequest<?> request;

      SentRequest(InternalRequest<?> request) {
         this.request = request;
      }

      /**
       * Complete the request with a {@link org.infinispan.util.concurrent.TimeoutException}
       */
      public void forceTimeout() {
         assertFalse(request.isDone());
         request.fail(log.requestTimedOut(-1, "Induced failure"));
      }

      /**
       * Wait for a response from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectResponse(Address sender, Consumer<Response> checker) throws InterruptedException {
         assertFalse(request.isDone());

         Response response = request.peekResponse(sender);
         log.debugf("Checking response for %s from %s: %s", request.getCommand(), sender, response);
         checker.accept(response);

         return new BlockedResponse(request, this, sender, response);
      }

      /**
       * Wait for a response from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectResponse(Address sender) throws InterruptedException {
         return expectResponse(sender, r -> {});
      }

      /**
       * Wait for a response from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectResponse(Address sender, Response expectedResponse)
         throws InterruptedException {
         return expectResponse(sender, r -> assertEquals(expectedResponse, r));
      }

      /**
       * Wait for a {@code CacheNotFoundResponse} from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectLeaver(Address a) throws InterruptedException {
         return expectResponse(a, CacheNotFoundResponse.INSTANCE);
      }

      /**
       * Wait for an {@code ExceptionResponse} from {@code sender}, but keep the request blocked.
       */
      public BlockedResponse expectException(Address a, Class<? extends Exception> expectedException)
         throws InterruptedException {
         return expectResponse(a, r -> {
            Exception exception = ((ExceptionResponse) r).getException();
            Exceptions.assertException(expectedException, exception);
         });
      }

      /**
       * Wait for all the responses.
       */
      public BlockedResponseMap awaitAll() throws InterruptedException {
         return awaitAll((sender, response) -> {});
      }

      /**
       * Wait for all the responses.
       */
      public BlockedResponseMap awaitAll(BiConsumer<Address, Response> checker) throws InterruptedException {
         assertFalse(request.resultFuture.isDone());

         Map<Address, Response> responseMap = request.peekFinish();
         responseMap.forEach(checker);
         return new BlockedResponseMap(request, this, responseMap);
      }

      /**
       * Wait for all the responses and process them.
       */
      public void receiveAll() throws InterruptedException {
         assertFalse(request.resultFuture.isDone());

         request.throwIfFailed();

         Map.Entry<Address, Response> entry;
         while ((entry = request.peekResponse()) != null) {
            Address sender = entry.getKey();
            Response response = entry.getValue();
            log.tracef("Receiving response for %s from %s: %s", request.getCommand(), sender, response);
            request.collectResponse(sender, response);
         }

         if (!request.isDone()) {
            request.collectFinish();
         }
      }

      /**
       * Complete a request after expecting and receiving responses individually, e.g. with
       * {@link #expectResponse(Address)}.
       *
       * This method blocks until all the responses have been received internally, but doesn't pass them on
       * to the original response collector (it only calls {@link ResponseCollector#finish()}).
       */
      public void finish() throws InterruptedException {
         assertFalse(request.resultFuture.isDone());

         request.peekFinish();
         request.collectFinish();
      }
   }

   public static class BlockedResponse {
      private InternalRequest<?> request;
      final SentRequest sentRequest;
      final Address sender;
      final Response response;

      private BlockedResponse(InternalRequest<?> request, SentRequest sentRequest, Address sender,
                              Response response) {
         this.request = request;
         this.sentRequest = sentRequest;
         this.sender = sender;
         this.response = response;
      }

      /**
       * Process a single response.
       * <p>
       * Note that processing the last response will NOT complete the request, you still need to call
       * {@link SentRequest#receiveAll()}.
       */
      public SentRequest receive() {
         log.tracef("Unblocking response from %s: %s", sender, response);
         request.collectResponse(this.sender, response);
         return sentRequest;
      }

      public SentRequest replace(Response newResponse) {
         log.tracef("Replacing response from %s: %s (was %s)", sender, newResponse, response);
         request.collectResponse(this.sender, newResponse);
         return sentRequest;
      }
   }

   public static class BlockedResponseMap {
      private final InternalRequest request;
      private final SentRequest sentRequest;
      private Map<Address, Response> responseMap;

      private BlockedResponseMap(InternalRequest request, SentRequest sentRequest,
                                 Map<Address, Response> responseMap) {
         this.request = request;
         this.sentRequest = sentRequest;
         this.responseMap = responseMap;
      }

      public void receive() {
         assertFalse(request.resultFuture.isDone());

         log.tracef("Unblocking responses for %s: %s", request.getCommand(), sentRequest);
         responseMap.forEach(request::collectResponse);
         request.collectFinish();
      }

      public void replace(Map<Address, Response> newResponses) {
         assertFalse(request.resultFuture.isDone());

         log.tracef("Replacing responses for %s: %s (was %s)", request.getCommand(), newResponses, sentRequest);
         newResponses.forEach(request::collectResponse);
         request.collectFinish();
      }

      public Map<Address, Response> getResponses() {
         return responseMap;
      }
   }

   public static class FakeResponses {
      private final InternalRequest<?> request;

      public FakeResponses(InternalRequest<?> request) {
         this.request = request;
      }

      public void receive(Map<Address, Response> responses) {
         log.tracef("Skipping request %s, using responses %s", request.getCommand(), responses);
         responses.forEach(request::collectResponse);
         if (!request.isDone()) {
            request.queueFinish();
            request.collectFinish();
         }
      }

      public void receive(Address sender, Response response) {
         receive(Collections.singletonMap(sender, response));
      }

      public void receive(Address sender1, Response response1, Address sender2, Response response2) {
         Map<Address, Response> responses = new LinkedHashMap<>();
         responses.put(sender1, response1);
         responses.put(sender2, response2);
         receive(responses);
      }

      public void receive(Address sender1, Response response1, Address sender2, Response response2, Address sender3,
                          Response response3) {
         Map<Address, Response> responses = new LinkedHashMap<>();
         responses.put(sender1, response1);
         responses.put(sender2, response2);
         responses.put(sender3, response3);
         receive(responses);
      }

      /**
       * Complete the request with a {@link org.infinispan.util.concurrent.TimeoutException}
       */
      public void forceTimeout() {
         fail(log.requestTimedOut(-1, "Induced failure"));
      }

      /**
       * Complete the request with a custom exception.
       */
      private void fail(Throwable e) {
         assertFalse(request.resultFuture.isDone());
         request.fail(e);
      }
   }
}
