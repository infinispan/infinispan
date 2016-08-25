package org.infinispan.remoting.transport.jgroups;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.infinispan.remoting.responses.Response;
import org.jgroups.SuspectedException;
import org.jgroups.util.Rsp;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class SingleResponseFuture extends CompletableFuture<Rsp<Response>>
      implements Callable<Void> {
   private final CompletableFuture<Response> request;
   private volatile Future<?> timeoutFuture = null;

   SingleResponseFuture(CompletableFuture<Response> request) {
      this.request = request;
      request.whenComplete(this::requestDone);
   }

   private void requestDone(Response response, Throwable throwable) {
      if (throwable == null) {
         complete(new Rsp<>(response));
      } else if (throwable instanceof SuspectedException) {
         Rsp<Response> rsp = new Rsp<>();
         rsp.setSuspected();
         complete(rsp);
      } else {
         complete(new Rsp<>(throwable));
      }
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
      complete(new Rsp<>());
      request.cancel(false);
      return null;
   }
}
