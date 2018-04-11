package org.infinispan.remoting.transport.impl;

import java.util.Set;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.AbstractRequest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Request implementation that waits for a response from a single target node.
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class SingleTargetRequest<T> extends AbstractRequest<T> {
   private static final Log log = LogFactory.getLog(SingleTargetRequest.class);

   private final Address target;

   public SingleTargetRequest(ResponseCollector<T> wrapper, long requestId, RequestRepository repository, Address target) {
      super(requestId, wrapper, repository);
      this.target = target;
   }

   @Override
   public void onResponse(Address sender, Response response) {
      if (!target.equals(sender)) {
         completeExceptionally(
               new IllegalStateException("Received response from " + sender + ", but target was " + target));
      }
      receiveResponse(sender, response);
   }

   @Override
   public boolean onNewView(Set<Address> members) {
      boolean targetIsMissing = !members.contains(target);
      if (targetIsMissing) {
         receiveResponse(target, CacheNotFoundResponse.INSTANCE);
      }
      return targetIsMissing;
   }

   private void receiveResponse(Address sender, Response response) {
      try {
         // Ignore the return value, we won't receive another response
         T result;
         synchronized (responseCollector) {
            result = responseCollector.addResponse(sender, response);
            if (result == null) {
               result = responseCollector.finish();
            }
         }
         complete(result);
      } catch (Exception e) {
         completeExceptionally(e);
      }
   }

   @Override
   protected void onTimeout() {
      completeExceptionally(log.requestTimedOut(requestId, target.toString()));
   }
}
