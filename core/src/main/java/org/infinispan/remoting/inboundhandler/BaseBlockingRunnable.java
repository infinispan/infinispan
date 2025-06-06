package org.infinispan.remoting.inboundhandler;

import static org.infinispan.remoting.inboundhandler.BasePerCacheInboundInvocationHandler.exceptionHandlingCommand;
import static org.infinispan.remoting.inboundhandler.BasePerCacheInboundInvocationHandler.interruptedException;
import static org.infinispan.remoting.inboundhandler.BasePerCacheInboundInvocationHandler.outdatedTopology;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.BlockingRunnable;

/**
 * Common logic to handle {@link org.infinispan.commands.remote.CacheRpcCommand}.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public abstract class BaseBlockingRunnable implements BlockingRunnable {

   protected final BasePerCacheInboundInvocationHandler handler;
   protected final CacheRpcCommand command;
   protected final Reply reply;
   protected final boolean sync;
   protected Response response;
   private final BiConsumer<Response, Throwable> handleBeforeInvoke = this::handleBeforeInvoke;

   protected BaseBlockingRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply,
                                  boolean sync) {
      this.handler = handler;
      this.command = command;
      this.reply = reply;
      this.sync = sync;
   }

   @Override
   public void run() {
      if (sync) {
         runSync();
      } else {
         runAsync();
      }
   }

   private void runSync() {
      try {
         CompletionStage<CacheNotFoundResponse> beforeStage = beforeInvoke();
         if (beforeStage != null) {
            response = CompletionStages.join(beforeStage);
            if (response != null) {
               afterInvoke();
               return;
            }
         }
         CompletableFuture<Response> commandFuture = handler.invokeCommand(command);
         response = commandFuture.join();
         afterInvoke();
      } catch (Throwable t) {
         afterCommandException(unwrap(t));
      } finally {
         if (handler.isStopped()) {
            response = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(response);
         onFinally();
      }
   }

   private void runAsync() {
      CompletionStage<CacheNotFoundResponse> beforeStage = beforeInvoke();
      if (beforeStage == null) {
         invoke();
      } else if (CompletionStages.isCompletedSuccessfully(beforeStage)) {
         handleBeforeInvoke(CompletionStages.join(beforeStage), null);
      } else {
         beforeStage.whenComplete(handleBeforeInvoke);
      }
   }

   private void handleBeforeInvoke(Response rsp, Throwable throwable) {
      if (rsp != null) {
         response = rsp;
         afterInvoke();
         if (handler.isStopped()) {
            response = rsp = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(rsp);
         onFinally();
      } else if (throwable != null) {
         afterCommandException(unwrap(throwable));
         if (handler.isStopped()) {
            response = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(response);
         onFinally();
      } else {
         invoke();
      }
   }

   private void invoke() {
      CompletableFuture<Response> commandFuture;
      try {
         commandFuture = handler.invokeCommand(command);
      } catch (Throwable t) {
         afterCommandException(unwrap(t));
         if (handler.isStopped()) {
            response = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(response);
         onFinally();
         return;
      }
      if (CompletionStages.isCompletedSuccessfully(commandFuture)) {
         invokedComplete(commandFuture.join(), null);
      } else {
         // Not worried about caching this method invocation, as this runnable is only used once
         commandFuture.whenComplete(this::invokedComplete);
      }
   }

   private void invokedComplete(Response rsp, Throwable throwable) {
      try {
         if (throwable == null) {
            response = rsp;
            afterInvoke();
         } else {
            afterCommandException(unwrap(throwable));
         }
      } finally {
         if (handler.isStopped()) {
            response = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(response);
         onFinally();
      }
   }

   private static Throwable unwrap(Throwable throwable) {
      if (throwable instanceof CompletionException && throwable.getCause() != null) {
         throwable = throwable.getCause();
      }
      return throwable;
   }

   private void afterCommandException(Throwable throwable) {
      if (throwable instanceof InterruptedException) {
         response = interruptedException(command);
      } else if (throwable instanceof OutdatedTopologyException) {
         response = outdatedTopology((OutdatedTopologyException) throwable);
      } else if (throwable instanceof IllegalLifecycleStateException) {
         response = CacheNotFoundResponse.INSTANCE;
      } else {
         response = exceptionHandlingCommand(command, throwable);
      }
      onException(throwable);
   }

   protected void onFinally() {
      //no-op by default
   }

   protected void onException(Throwable throwable) {
      //no-op by default
   }

   protected void afterInvoke() {
      //no-op by default
   }

   protected CompletionStage<CacheNotFoundResponse> beforeInvoke() {
      return null; //no-op by default
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{command=" + command +
            ", sync=" + sync +
            '}';
   }
}
