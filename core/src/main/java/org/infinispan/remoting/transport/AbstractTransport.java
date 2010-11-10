package org.infinispan.remoting.transport;

import org.infinispan.CacheException;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.remoting.ReplicationException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;

import java.util.List;

/**
 * Common transport-related behaviour
 *
 * @author Manik Surtani
 * @version 4.2
 */
public abstract class AbstractTransport implements Transport {

   protected GlobalConfiguration configuration;

   public void setConfiguration(GlobalConfiguration globalConfiguration) {
      this.configuration = globalConfiguration;
   }

   protected final boolean shouldThrowException(Exception ce) {
      if (!configuration.isStrictPeerToPeer()) {
         if (ce instanceof NamedCacheNotFoundException) return false;
         if (ce.getCause() != null && ce.getCause() instanceof NamedCacheNotFoundException) return false;
      }
      return true;
   }

   protected boolean parseResponseAndAddToResponseList(Object responseObject, List<Response> responseListToAddTo, boolean wasSuspected,
                                                       boolean wasReceived, Address sender, boolean usedResponseFilter)
           throws Exception
   {
      Log log = getLog();
      boolean trace = log.isTraceEnabled();
      boolean invalidResponse = true;
      if (wasSuspected || !wasReceived) {
         if (wasSuspected) {
            throw new SuspectException("Suspected member: " + sender);
         } else {
            // if we have a response filter then we may not have waited for some nodes!
            if (!usedResponseFilter) throw new TimeoutException("Replication timeout for " + sender);
         }
      } else {
         invalidResponse = false;
         if (responseObject instanceof Response) {
            Response response = (Response) responseObject;
            if (response instanceof ExceptionResponse) {
               Exception e = ((ExceptionResponse) response).getException();
               if (!(e instanceof ReplicationException)) {
                  // if we have any application-level exceptions make sure we throw them!!
                  if (shouldThrowException(e)) {
                     throw e;
                  } else {
                     if (log.isDebugEnabled()) log.debug("Received exception from sender {0}", sender, e);
                  }
               }
            }
            responseListToAddTo.add(response);
         } else if (responseObject instanceof Exception) {
            Exception e = (Exception) responseObject;
            if (trace) log.trace("Unexpected exception from " + sender, e);
            throw e;
         } else if (responseObject instanceof Throwable) {
            Throwable t = (Throwable) responseObject;
            if (trace) log.trace("Unexpected throwable from " + sender, t);
            throw new CacheException("Remote (" + sender + ") failed unexpectedly", t);
         }
      }
      
      return invalidResponse;
   }
}
