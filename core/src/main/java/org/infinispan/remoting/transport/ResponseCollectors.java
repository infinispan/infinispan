package org.infinispan.remoting.transport;

import static org.infinispan.util.logging.Log.CLUSTER;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.CrossSiteIllegalLifecycleStateException;
import org.infinispan.commons.util.Experimental;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;

/**
 * @author Dan Berindei
 * @since 9.1
 */
@Experimental
public class ResponseCollectors {
   public static <S> CacheException wrapRemoteException(S sender, Throwable exception) {
      CacheException e;
      if (exception instanceof SuspectException) {
         e = CLUSTER.thirdPartySuspected(sender, (SuspectException) exception);
      } else if (exception instanceof AvailabilityException || exception instanceof OutdatedTopologyException || exception instanceof CrossSiteIllegalLifecycleStateException) {
         e = (CacheException) exception;
      } else {
         // if we have any application-level exceptions make sure we throw them!!
         e = CLUSTER.remoteException(sender, exception);
      }
      return e;
   }

   public static <S> SuspectException remoteNodeSuspected(S sender) {
      return CLUSTER.remoteNodeSuspected(sender);
   }
}
