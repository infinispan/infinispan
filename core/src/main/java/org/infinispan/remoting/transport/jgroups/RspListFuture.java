package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.Response;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.FutureListener;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Dan Berindei
 * @since 8.0
 */
class RspListFuture extends CompletableFuture<RspList<Response>> implements FutureListener<RspList<Response>> {
   private final GroupRequest<Response> request;

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
   }

   public void timeout() {
      futureDone(null);
      request.cancel(true);
   }
}
