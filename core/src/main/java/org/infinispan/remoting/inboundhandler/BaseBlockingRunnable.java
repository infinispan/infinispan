package org.infinispan.remoting.inboundhandler;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
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

   private final Address origin;

   protected BaseBlockingRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply, Address origin) {
      this.handler = handler;
      this.command = command;
      this.reply = reply;
      this.origin = origin;
   }

   @Override
   public void run() {
      try {
         response = beforeInvoke();
         if (response == null) {
            response = handler.invokePerform(command, origin);
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
         reply.reply(response);
         onFinally();
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

   protected Response beforeInvoke() throws Exception {
      return null; //no-op by default
   }
}
