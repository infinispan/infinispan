package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.Response;
import org.jgroups.blocks.UnicastRequest;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Rsp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Dan Berindei
 * @since 8.0
 */
class SingleResponseFuture extends CompletableFuture<Rsp<Response>> implements FutureListener<Response> {
   private final UnicastRequest request;
   private volatile Future<?> timeoutFuture = null;

   SingleResponseFuture(NotifyingFuture<Response> request) {
      this.request = ((UnicastRequest) request);
      request.setListener(this);
   }

   @Override
   public void futureDone(Future<Response> future) {
      Rsp<Response> response = request.getResult();
      complete(response);
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
      }
   }

   public void timeout() {
      complete(request.getResult());
      request.cancel(false);
   }

   public void setTimeoutFuture(Future<?> timeoutFuture) {
      this.timeoutFuture = timeoutFuture;
      if (isDone()) {
         timeoutFuture.cancel(false);
      }
   }
}
