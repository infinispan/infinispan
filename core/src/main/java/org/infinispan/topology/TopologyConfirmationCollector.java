package org.infinispan.topology;

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

/**
 * It collects the confirmation of the first two phases in the topology process.
 * <p/>
 * This class is <b>not</b> thread safe!
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 7.1
 */
@NotThreadSafe
class TopologyConfirmationCollector {

   private final static Log log = LogFactory.getLog(TopologyConfirmationCollector.class);
   private static final boolean trace = log.isTraceEnabled();

   private final String cacheName;
   private final int topologyId;
   private final Set<Address> confirmationsNeeded;
   private Runnable runnable;

   /**
    * Create a new {@link org.infinispan.topology.TopologyConfirmationCollector}.
    *
    * @param cacheName  the cache name.
    * @param topologyId the current topology id.
    * @param members    the members which needs to send the confirmation.
    * @param runnable   the {@link java.lang.Runnable} that is triggered when all the confirmations are received.
    */
   public TopologyConfirmationCollector(String cacheName, int topologyId, Collection<Address> members, Runnable runnable) {
      this.cacheName = cacheName;
      this.topologyId = topologyId;
      this.runnable = runnable;
      this.confirmationsNeeded = new HashSet<>(members);

      if (trace) {
         log.tracef("Initialized topology confirmation collector %d@%s, initial list is %s",
                    topologyId, cacheName, confirmationsNeeded);
      }
   }

   /**
    * It confirms the topology for the {@code receivedTopologyId} from {@code node}.
    * <p/>
    * When all the confirmations has been received, it triggers the runnable.
    */
   public void confirm(Address node, int receivedTopologyId) {
      checkTopology(receivedTopologyId, node);

      if (confirmationsNeeded.remove(node)) {
         if (trace) {
            log.tracef("Topology confirmation collector %d@%s received confirmation for %s, remaining list is %s",
                       topologyId, cacheName, node, confirmationsNeeded);
         }

         checkAndRun();
      } else {
         if (trace) {
            log.tracef("Topology confirmation collector %d@%s ignored confirmation for %s, which is already confirmed",
                       topologyId, cacheName, node);
         }
      }
   }

   /**
    * It updates the members by removing the leavers.
    */
   public void updateMembers(Collection<Address> newMembers) {
      confirmationsNeeded.retainAll(newMembers);

      if (trace) {
         log.tracef("Topology confirmation collector %d@%s members list updated, remaining list is %s",
                    topologyId, cacheName, confirmationsNeeded);
      }

      checkAndRun();
   }

   @Override
   public String toString() {
      return "TopologyConfirmationCollector{" +
            "cacheName=" + cacheName +
            "topologyId=" + topologyId +
            ", confirmationsNeeded=" + confirmationsNeeded +
            '}';
   }

   private void checkTopology(int receivedTopologyId, Address node) throws CacheException {
      if (topologyId > receivedTopologyId) {
         throw new CacheException(
               format("Received invalid topology confirmation from %s for cache %s, expecting topology id %d but got %d",
                      node, cacheName, topologyId, receivedTopologyId));
      }
   }

   private void checkAndRun() {
      if (confirmationsNeeded.isEmpty() && runnable != null) {
         if (trace) {
            log.tracef("Topology confirmation collector %d@%s is about to trigger the runnable!", topologyId, cacheName);
         }
         runnable.run();
         runnable = null;
      }
   }
}
