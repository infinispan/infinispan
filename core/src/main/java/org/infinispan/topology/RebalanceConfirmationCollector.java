package org.infinispan.topology;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
* Created with
*
* @author Dan Berindei
* @since 5.2
*/
class RebalanceConfirmationCollector {
   private final static Log log = LogFactory.getLog(RebalanceConfirmationCollector.class);

   private final String cacheName;
   private final int topologyId;
   private final Set<Address> confirmationsNeeded;
   private final Runnable whenCompleted;

   public RebalanceConfirmationCollector(String cacheName, int topologyId, Collection<Address> members, Runnable whenCompleted) {
      this.cacheName = cacheName;
      this.topologyId = topologyId;
      this.confirmationsNeeded = new HashSet<Address>(members);
      this.whenCompleted = whenCompleted;
      log.tracef("Initialized topology confirmation collector %d@%s, initial list is %s",
            topologyId, cacheName, confirmationsNeeded);
   }

   /**
    * @return {@code true} if everyone has confirmed
    */
   public void confirmPhase(Address node, int receivedTopologyId) {
      synchronized (this) {
         if (topologyId > receivedTopologyId) {
            throw new CacheException(String.format("Received invalid rebalance confirmation from %s " +
                  "for cache %s, expecting topology id %d but got %d", node, cacheName, topologyId, receivedTopologyId));
         }

         boolean removed = confirmationsNeeded.remove(node);
         if (!removed) {
            log.tracef("Rebalance confirmation collector %d@%s ignored confirmation for %s, which is already confirmed",
                  topologyId, cacheName, node);
            return;
         }

         log.tracef("Rebalance confirmation collector %d@%s received confirmation for %s, remaining list is %s",
               topologyId, cacheName, node, confirmationsNeeded);
         if (confirmationsNeeded.isEmpty()) {
            whenCompleted.run();
         }
      }
   }

   /**
    * @return {@code true} if everyone has confirmed
    */
   public void updateMembers(Collection<Address> newMembers) {
      synchronized (this) {
         // only return true the first time
         boolean modified = confirmationsNeeded.retainAll(newMembers);
         log.tracef("Rebalance confirmation collector %d@%s members list updated, remaining list is %s",
               topologyId, cacheName, confirmationsNeeded);
         if (modified && confirmationsNeeded.isEmpty()) {
            whenCompleted.run();
         }
      }
   }

   @Override
   public String toString() {
      synchronized (this) {
         return "RebalanceConfirmationCollector{" +
               "cacheName=" + cacheName +
               ", topologyId=" + topologyId +
               ", confirmationsNeeded=" + confirmationsNeeded +
               '}';
      }
   }
}
