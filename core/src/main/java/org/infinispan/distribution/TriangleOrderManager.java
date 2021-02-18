package org.infinispan.distribution;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * It manages the order of updates from the primary owner to backup owner.
 * <p>
 * It depends on the cache topology id. The primary owner assigns the sequence number to the backup command and then
 * sends it to the backup owner. In the backup owner, the command awaits until it is its turn to be executed.
 * <p>
 * If the command topology id does not match, it throws an {@link OutdatedTopologyException}.
 * <p>
 * The sequence order starts with 1 and it is per segment based. This allows segments to be updated concurrently.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
public class TriangleOrderManager {

   private static final Log log = LogFactory.getLog(TriangleOrderManager.class);
   private final TriangleSequencer[] sequencers;
   @Inject DistributionManager distributionManager;

   public TriangleOrderManager(int segments) {
      TriangleSequencer[] triangleSequencers = new TriangleSequencer[segments];
      for (int segment = 0; segment < segments; ++segment) {
         triangleSequencers[segment] = new TriangleSequencer(segment);
      }
      sequencers = triangleSequencers;
   }

   public long next(int segmentId, final int commandTopologyId) {
      checkTopologyId(commandTopologyId);
      try {
         return getNext(segmentId, commandTopologyId);
      } finally {
         //check if topology didn't change in the meanwhile
         checkTopologyId(commandTopologyId);
      }
   }

   public boolean isNext(int segmentId, long sequenceNumber, int commandTopologyId) {
      final int topologyId = distributionManager.getCacheTopology().getTopologyId();
      return commandTopologyId < topologyId ||
             (commandTopologyId == topologyId && checkIfNext(segmentId, commandTopologyId, sequenceNumber));
   }

   public void markDelivered(int segmentId, long sequenceNumber, int commandTopologyId) {
      sequencers[segmentId].deliver(commandTopologyId, sequenceNumber);
   }

   /**
    * Meant for testing only.
    *
    * @return The latest sequence number sent for segment {@code segmentId} in topology {@code topologyId}.
    */
   public long latestSent(int segmentId, int topologyId) {
      return sequencers[segmentId].latestSent(topologyId);
   }

   private long getNext(int segmentId, int topologyId) {
      return sequencers[segmentId].next(topologyId);
   }

   private boolean checkIfNext(int segmentId, int topologyId, long sequenceNumber) {
      return sequencers[segmentId].isNext(topologyId, sequenceNumber);
   }

   private void checkTopologyId(int topologyId) {
      if (topologyId != distributionManager.getCacheTopology().getTopologyId()) {
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }
   }

   private static class TriangleSequencer {
      private final int segment;
      @GuardedBy("this")
      private int senderTopologyId = -1;
      @GuardedBy("this")
      private int receiverTopologyId = -1;
      @GuardedBy("this")
      private long senderSequenceNumber = 1;
      @GuardedBy("this")
      private long receiverSequenceNumber = 1;

      private TriangleSequencer(int segment) {
         this.segment = segment;
      }

      private synchronized long next(int commandTopologyId) {
         if (senderTopologyId == commandTopologyId) {
            if (log.isTraceEnabled()) {
               log.tracef("Sender %d new sequence %d:%d", segment, senderTopologyId, senderSequenceNumber);
            }
            return senderSequenceNumber++;
         } else if (senderTopologyId < commandTopologyId) {
            if (log.isTraceEnabled()) {
               log.tracef("Sender %d new sequence %d:1 (changed topology from %d)",
                          segment, senderTopologyId, commandTopologyId);
            }
            //update topology. this command will be the first
            senderTopologyId = commandTopologyId;
            senderSequenceNumber = 2;
            return 1;
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Sender %d retrying because of outdated topology: %d < %d",
                          segment, commandTopologyId, senderTopologyId);
            }
            //this topology is higher than the command topology id.
            //another topology was installed. this command will fail with OutdatedTopologyException.
            throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
         }
      }

      private synchronized long latestSent(int topologyId) {
         if (topologyId < senderTopologyId)
            throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;

         if (senderTopologyId < topologyId)
            return 0;

         return senderSequenceNumber - 1;
      }

      private synchronized void deliver(int commandTopologyId, long sequenceNumber) {
         if (receiverTopologyId == commandTopologyId && receiverSequenceNumber == sequenceNumber) {
            receiverSequenceNumber++;
            if (log.isTraceEnabled()) {
               log.tracef("Receiver %d delivered sequence %d:%d", segment, commandTopologyId, sequenceNumber);
            }
         }
      }

      private synchronized boolean isNext(int commandTopologyId, long sequenceNumber) {
         if (log.isTraceEnabled()) {
            log.tracef("Receiver %d checking sequence %d:%d, current sequence is %d:%d",
                       segment, commandTopologyId, sequenceNumber, receiverTopologyId, receiverSequenceNumber);
         }
         if (receiverTopologyId == commandTopologyId) {
            return receiverSequenceNumber == sequenceNumber;
         } else if (receiverTopologyId < commandTopologyId) {
            receiverTopologyId = commandTopologyId;
            receiverSequenceNumber = 1;
            return 1 == sequenceNumber;
         } else {
            //this topology is higher than the command topology id.
            //another topology was installed. this command will fail with OutdatedTopologyException.
            return true;
         }
      }
   }
}
