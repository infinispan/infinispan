package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;

/**
 * An empty implementation of {@link RequestTracker}.
 * <p>
 * It does not keep track of any metric information about the request and all the operations are no-op. The method
 * {@link #destination()} returns the destination {@link Address}.
 */
record NoOpRequestTracker(Address destination) implements RequestTracker {

   @Override
   public void resetSendTime() {

   }

   @Override
   public void onComplete() {

   }

   @Override
   public void onTimeout() {

   }
}
