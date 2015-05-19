package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.Response;
import org.jgroups.blocks.UnicastRequest;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Rsp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Dan Berindei
 * @since 8.0
 */
class SingleResponseFuture extends CompletableFuture<Rsp<Response>> implements FutureListener<Response> {
   private final UnicastRequest request;

   SingleResponseFuture(NotifyingFuture<Response> request) {
      this.request = ((UnicastRequest) request);
      request.setListener(this);
   }

   @Override
   public void futureDone(Future<Response> future) {
      Rsp<Response> response = request.getResult();
      complete(response);
   }

   public void timeout() {
      complete(request.getResult());
      request.cancel(false);
   }
}
