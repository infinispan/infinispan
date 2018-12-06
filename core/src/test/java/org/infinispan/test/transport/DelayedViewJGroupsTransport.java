package org.infinispan.test.transport;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.View;

public final class DelayedViewJGroupsTransport extends JGroupsTransport {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final CountDownLatch waitLatch;
   private boolean unblocked;

   public DelayedViewJGroupsTransport(CountDownLatch waitLatch) {
      this.waitLatch = waitLatch;
   }

   @Override
   public void receiveClusterView(View newView) {
      // check if this is an event of node going down, and if so wait for a signal to apply new view
      if (waitLatch != null && getMembers().size() > newView.getMembers().size()) {
         try {
            log.debugf("Delaying view %s", newView);
            unblocked = waitLatch.await(10, TimeUnit.SECONDS);
            if (unblocked) {
               log.debugf("Unblocking view %s", newView);
            } else {
               log.errorf("Timed out waiting for view to be unblocked: %s", newView);
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      super.receiveClusterView(newView);
   }

   public void assertUnblocked() {
      assert unblocked;
   }
}
