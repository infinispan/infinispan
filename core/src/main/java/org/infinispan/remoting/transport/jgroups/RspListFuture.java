package org.infinispan.remoting.transport.jgroups;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import org.infinispan.remoting.responses.Response;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.RspList;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class RspListFuture extends CompletableFuture<Responses>
      implements Callable<Void>, BiConsumer<RspList<Response>, Throwable> {
   private final GroupRequest<Response> request;
   private volatile Future<?> timeoutFuture = null;

   RspListFuture(GroupRequest<Response> request) {
      this.request = request;
      request.whenComplete(this);
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
      completeExceptionally(new TimeoutException("Timed out waiting for responses"));
      request.cancel(true);
      return null;
   }

   @Override
   public void accept(RspList<Response> rsps, Throwable throwable) {
      requestDone(rsps, throwable);
   }

   private void requestDone(RspList<Response> rsps, Throwable throwable) {
      if (throwable == null) {
         complete(new Responses(rsps));
      } else {
         completeExceptionally(throwable);
      }
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
      }
   }
}
