package org.infinispan.remoting.inboundhandler;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.BlockingRunnable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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

   protected BaseBlockingRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply,
                                  boolean sync) {
      this.handler = handler;
      this.command = command;
      this.reply = reply;
      this.sync = sync;
   }

   @Override
   public void run() {
      try {
         response = beforeInvoke();
         if (response != null) {
            reply.reply(response);
            onFinally();
            return;
         }

         CompletableFuture<Response> future = handler.invokeCommand(command);
         if (sync) {
            response = future.join();
            afterInvoke();
            reply.reply(response);
            onFinally();
            return;
         }

         future.whenComplete((response1, throwable) -> {
            try {
               if (throwable == null) {
                  response = response1;
                  afterInvoke();
               } else {
                  if (throwable instanceof CompletionException) {
                     throwable = throwable.getCause();
                  }
                  afterCommandException(throwable);
               }
            } finally {
               reply.reply(response);
               onFinally();
            }
         });
      } catch (Throwable t) {
         if (t.getCause() != null && t instanceof CompletionException) {
            t = t.getCause();
         }
         afterCommandException(t);
         // We didn't get a CompletableFuture from invokeCommand, so we have to send the reply here
         reply.reply(response);
         onFinally();
      }
   }

   private void afterCommandException(Throwable throwable) {
      if (throwable instanceof InterruptedException) {
         response = handler.interruptedException(command);
      } else if (throwable instanceof OutdatedTopologyException) {
         response = handler.outdatedTopology((OutdatedTopologyException) throwable);
      } else if (throwable instanceof IllegalLifecycleStateException) {
         response = CacheNotFoundResponse.INSTANCE;
      } else {
         response = handler.exceptionHandlingCommand(command, throwable);
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

   protected Response beforeInvoke() throws Exception {
      return null; //no-op by default
   }
}
