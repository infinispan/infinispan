package org.infinispan.remoting.transport.jgroups;

import java.util.concurrent.Executor;

import org.jgroups.Address;
import org.jgroups.View;
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
   private final Executor remoteExecutor;

   public CustomRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr,
                                  Executor remoteExecutor) {
      // Make sure we use the same protocol id as the parent class
      super(ClassConfigurator.getProtocolId(RequestCorrelator.class), transport, handler, local_addr);
      this.remoteExecutor = remoteExecutor;
   }

   @Override
   public void receiveSuspect(Address mbr) {
      // Ignore suspect events, if the node actually left that will be handled by receiveView(View)
   }

   @Override
   public void receiveView(View new_view) {
      // Suspecting a node may unblock some commands, which can potentially block that thread for a long time.
      // We don't want to block view handling, so we unblock the commands on a separate thread.
      // Ideally, we'd unblock each command on a separate thread.
      // For regular responses, it's ok to block the OOB thread that received the response:
      remoteExecutor.execute(() -> super.receiveView(new_view));
   }
}
