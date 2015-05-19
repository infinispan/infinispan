package org.infinispan.remoting.transport;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;

/**
 * Common transport-related behaviour
 *
 * @author Manik Surtani
 * @version 4.2
 */
public abstract class AbstractTransport implements Transport {

   protected GlobalConfiguration configuration;

   @Inject
   @SuppressWarnings("unused")
   public void setConfiguration(GlobalConfiguration globalConfiguration) {
      this.configuration = globalConfiguration;
   }

   public Response checkResponse(Object responseObject, Address sender, boolean ignoreCacheNotFoundResponse) {
      Log log = getLog();
      if (responseObject == null) {
         return SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
      } else if (responseObject instanceof Response) {
         Response response = (Response) responseObject;
         if (response instanceof ExceptionResponse) {
            ExceptionResponse exceptionResponse = (ExceptionResponse) response;
            Exception e = exceptionResponse.getException();
            if (e instanceof SuspectException) throw log.thirdPartySuspected(sender, (SuspectException) e);
            if (e instanceof AvailabilityException || e instanceof OutdatedTopologyException) throw (CacheException) e;

            // if we have any application-level exceptions make sure we throw them!!
            throw log.remoteException(sender, e);
         } else if (!ignoreCacheNotFoundResponse && response instanceof CacheNotFoundResponse) {
            throw new SuspectException("Cache not running on node " + sender, sender);
         }
         return response;
      } else {
         // All other responses should trigger an exception
         Class<?> responseClass = responseObject.getClass();
         log.tracef("Unexpected response object type from %s: %s", sender, responseClass);
         throw new CacheException(String.format("Unexpected response object type from %s: %s", sender, responseClass));
      }
   }

}
