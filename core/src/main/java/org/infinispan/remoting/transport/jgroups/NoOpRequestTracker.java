package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;

/**
 * An empty implementation of {@link RequestTracker}.
 * <p>
 * It does not keep track of any metric information about the request and all the operations are no-op. The method
 * {@link #destination()} returns the destination {@link Address}.
 */
class NoOpRequestTracker implements RequestTracker {

   private final Address destination;

   NoOpRequestTracker(Address destination) {
      this.destination = destination;
   }

   @Override
   public Address destination() {
      return destination;
   }

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
