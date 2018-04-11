package org.infinispan.remoting.transport.jgroups;

import java.util.Set;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.AbstractRequest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Request implementation that waits for a response from a single target node.
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class SingleSiteRequest<T> extends AbstractRequest<T> {
   private static final Log log = LogFactory.getLog(SingleSiteRequest.class);

   private final String site;

   SingleSiteRequest(ResponseCollector<T> wrapper, long requestId, RequestRepository repository, String site) {
      super(requestId, wrapper, repository);
      this.site = site;
   }

   @Override
   public void onResponse(Address sender, Response response) {
      receiveResponse(sender, response);
   }

   @Override
   public boolean onNewView(Set<Address> members) {
      // Ignore cluster views.
      return false;
   }

   private void receiveResponse(Address sender, Response response) {
      try {
         // Ignore the return value, we won't receive another response
         T result;
         synchronized (responseCollector) {
            if (isDone()) {
               throw new IllegalStateException(
                     "Duplicate response received for x-site request " + requestId + " from " + sender);
            }
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
      completeExceptionally(log.requestTimedOut(requestId, site));
   }

   public void sitesUnreachable(Set<String> sites) {
      if (sites.contains(site)) {
         receiveResponse(null, CacheNotFoundResponse.INSTANCE);
      }
   }
}
