package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.Response;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.FutureListener;
import org.jgroups.util.RspList;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Dan Berindei
 * @since 8.0
 */
class RspListFuture extends CompletableFuture<RspList<Response>> implements FutureListener<RspList<Response>>,
      Callable<Void> {
   private volatile GroupRequest<Response> request;
   private volatile Future<?> timeoutFuture = null;

   RspListFuture() {
   }

   /**
    * Add a reference to the request.
    *
    * Must be called before scheduling the timeout task.
    */
   public void setRequest(GroupRequest<Response> request) {
      this.request = request;
   }

   @Override
   public void futureDone(Future<RspList<Response>> future) {
      // The request field may not be set at this time
      // The future may be a
      RspList<Response> rspList;
      try {
         rspList = future.get();
         complete(rspList);
         if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
         }
      } catch (Throwable t) {
         completeExceptionally(t);
      }
   }

   public void setTimeoutFuture(Future<?> timeoutFuture) {
      this.timeoutFuture = timeoutFuture;
      if (isDone()) {
         timeoutFuture.cancel(false);
      }
   }

   @Override
   public Void call() throws Exception {
      // The request timed out
      complete(request.getResults());
      request.cancel(true);
      return null;
   }
}
