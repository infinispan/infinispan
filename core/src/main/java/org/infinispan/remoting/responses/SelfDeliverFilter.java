package org.infinispan.remoting.responses;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * Used in Total Order based protocol.
 * <p/>
 * This filter awaits until the command is deliver and processed by the local node.
 * <p/>
 * Warning: Non-Total Order command are not self delivered
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class SelfDeliverFilter implements TimeoutValidationResponseFilter {

   private final Address localAddress;
   private boolean selfDelivered;

   public SelfDeliverFilter(Address localAddress) {
      this.localAddress = localAddress;
      this.selfDelivered = false;
   }

   @Override
   public boolean isAcceptable(Response response, Address sender) {
      if (sender.equals(localAddress)) {
         selfDelivered = true;
      }
      return true;
   }

   @Override
   public boolean needMoreResponses() {
      return !selfDelivered;
   }

   @Override
   public void validate() throws TimeoutException {
      if (!selfDelivered) {
         throw new TimeoutException("Timeout waiting for member " + localAddress);
      }
   }
}
