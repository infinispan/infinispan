package org.infinispan.test.transport;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.View;

public final class DelayedViewJGroupsTransport extends JGroupsTransport {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final CompletableFuture<Void> waitLatch;

   public DelayedViewJGroupsTransport(CompletableFuture<Void> waitLatch) {
      this.waitLatch = waitLatch;
   }

   @Override
   public void receiveClusterView(View newView) {
      // check if this is an event of node going down, and if so wait for a signal to apply new view
      if (getMembers().size() > newView.getMembers().size()) {
         log.debugf("Delaying view %s", newView);
         waitLatch.thenAccept(__ -> {
            log.debugf("Unblocking view %s", newView);
            super.receiveClusterView(newView);
         });
      } else {
         super.receiveClusterView(newView);
      }
   }

   public void assertUnblocked() {
      assert waitLatch.isDone();
   }
}
