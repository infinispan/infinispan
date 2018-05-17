package org.infinispan.test.transport;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.View;

public final class DelayedViewJGroupsTransport extends JGroupsTransport {

   private final CountDownLatch waitLatch;

   public DelayedViewJGroupsTransport(CountDownLatch waitLatch) {
      this.waitLatch = waitLatch;
   }

   @Override
   public void receiveClusterView(View newView) {
      // check if this is an event of node going down, and if so wait for a signal to apply new view
      if (waitLatch != null && getMembers().size() > newView.getMembers().size()) {
         try {
            waitLatch.await(10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      super.receiveClusterView(newView);
   }
}
