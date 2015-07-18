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
   private final GroupRequest<Response> request;
   private volatile Future<?> timeoutFuture = null;

   RspListFuture(GroupRequest<Response> request) {
      this.request = request;
      if (request != null) {
         request.setListener(this);
      }
   }

   @Override
   public void futureDone(Future<RspList<Response>> future) {
      RspList<Response> responses = request.getResults();
      complete(responses);
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
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
