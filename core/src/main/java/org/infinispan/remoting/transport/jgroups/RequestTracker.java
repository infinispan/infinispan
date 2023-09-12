package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;

/**
 * A tracker for synchronous requests.
 */
public interface RequestTracker {

   /**
    * @return The destination {@link Address} of the request. It is never {@code null}.
    */
   Address destination();

   /**
    * Resets the send time to the current time.
    */
   // for staggered sends
   void resetSendTime();

   /**
    * The reply was just received.
    */
   void onComplete();

   /**
    * The request timed-out before receiving the reply.
    */
   void onTimeout();

}
