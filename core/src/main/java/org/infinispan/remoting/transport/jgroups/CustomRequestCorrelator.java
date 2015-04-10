package org.infinispan.remoting.transport.jgroups;

import org.jgroups.Address;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;

/**
 * Extend {@link RequestCorrelator} to ignore {@link org.jgroups.Event#SUSPECT} events.
 *
 * {@link SuspectException} will only be thrown when the target actually leaves the view.
 *
 * @author Dan Berindei
 * @since 7.2
 */
public class CustomRequestCorrelator extends RequestCorrelator {
   public CustomRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr) {
      // Make sure we use the same protocol id as the parent class
      super(ClassConfigurator.getProtocolId(RequestCorrelator.class), transport, handler, local_addr);
   }

   @Override
   public void receiveSuspect(Address mbr) {
      // Ignore suspect events, if the node actually left that will be handled by receiveView(View)
   }
}
