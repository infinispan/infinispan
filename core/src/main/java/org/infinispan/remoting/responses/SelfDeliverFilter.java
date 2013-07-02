package org.infinispan.remoting.responses;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;

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
public class SelfDeliverFilter implements ResponseFilter {

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
}
