package org.infinispan.remoting.inboundhandler;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.remote.CacheRpcCommand;
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
   protected Response response;

   protected BaseBlockingRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply) {
      this.handler = handler;
      this.command = command;
      this.reply = reply;
   }

   @Override
   public void run() {
      InvocationStatus invocationStatus = null;
      try {
         invocationStatus = beforeInvoke();
         switch (invocationStatus) {
            case CONTINUE:
               response = handler.invokePerform(command);
               break;
            case CACHE_NOT_FOUND:
               response = CacheNotFoundResponse.INSTANCE;
               break;
            case DEFERRED:
               return;
         }
         afterInvoke();
      } catch (InterruptedException e) {
         response = handler.interruptedException(command);
         onException(e);
      } catch (OutdatedTopologyException oe) {
         response = handler.outdatedTopology(oe);
         onException(oe);
      } catch (IllegalLifecycleStateException e) {
         response = CacheNotFoundResponse.INSTANCE;
         onException(e);
      } catch (Exception e) {
         response = handler.exceptionHandlingCommand(command, e);
         onException(e);
      } catch (Throwable throwable) {
         response = handler.exceptionHandlingCommand(command, throwable);
         onException(throwable);
      } finally {
         if (invocationStatus != InvocationStatus.DEFERRED) {
            reply.reply(response);
            onFinally();
         }
      }
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

   protected InvocationStatus beforeInvoke() throws Exception {
      return InvocationStatus.CONTINUE; //no-op by default
   }

   protected enum InvocationStatus {
      CONTINUE,
      CACHE_NOT_FOUND,
      DEFERRED
   }
}
