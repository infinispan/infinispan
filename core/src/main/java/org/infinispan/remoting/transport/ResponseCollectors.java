package org.infinispan.remoting.transport;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Experimental;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Dan Berindei
 * @since 9.1
 */
@Experimental
public class ResponseCollectors {
   private static final Log log = LogFactory.getLog(ResponseCollectors.class);

   public static CacheException wrapRemoteException(Address sender, Throwable exception) {
      CacheException e;
      if (exception instanceof SuspectException) {
         e = log.thirdPartySuspected(sender, (SuspectException) exception);
      } else if (exception instanceof AvailabilityException || exception instanceof OutdatedTopologyException) {
         e = (CacheException) exception;
      } else {
         // if we have any application-level exceptions make sure we throw them!!
         e = log.remoteException(sender, exception);
      }
      return e;
   }

   public static SuspectException remoteNodeSuspected(Address sender) {
      return log.remoteNodeSuspected(sender);
   }

   public static RuntimeException unexpectedResponse(Response response) {
      return new IllegalArgumentException("Unexpected response " + response);
   }
}
