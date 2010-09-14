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

   protected boolean parseResponseAndAddToResponseList(Object value, List<Response> retval, boolean wasSuspected,
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
            if (usedResponseFilter) throw new TimeoutException("Replication timeout for " + sender);
         }
      } else {
         invalidResponse = false;
         if (value instanceof Response) {
            Response response = (Response) value;
            if (response instanceof ExceptionResponse) {
               Exception e = ((ExceptionResponse) value).getException();
               if (!(e instanceof ReplicationException)) {
                  // if we have any application-level exceptions make sure we throw them!!
                  if (shouldThrowException(e)) {
                     throw e;
                  } else {
                     if (log.isDebugEnabled()) log.debug("Received exception from sender {0}", sender, e);
                  }
               }
            }
            retval.add(response);
         } else if (value instanceof Exception) {
            Exception e = (Exception) value;
            if (trace) log.trace("Unexpected exception from " + sender, e);
            throw e;
         } else if (value instanceof Throwable) {
            Throwable t = (Throwable) value;
            if (trace) log.trace("Unexpected throwable from " + sender, t);
            throw new CacheException("Remote (" + sender + ") failed unexpectedly", t);
         }
      }
      
      return invalidResponse;
   }
}
