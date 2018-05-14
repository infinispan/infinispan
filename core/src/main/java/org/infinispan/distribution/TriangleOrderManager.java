package org.infinispan.distribution;

import net.jcip.annotations.GuardedBy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
public class TriangleOrderManager {

   private static final Log log = LogFactory.getLog(TriangleOrderManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private final TriangleSequencer[] sequencers;
   @Inject private DistributionManager distributionManager;

   public TriangleOrderManager(int segments) {
      TriangleSequencer[] triangleSequencers = new TriangleSequencer[segments];
      for (int i = 0; i < segments; ++i) {
         triangleSequencers[i] = new TriangleSequencer();
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

   private long getNext(int segmentId, int topologyId) {
      return sequencers[segmentId].next(topologyId);
   }

   private boolean checkIfNext(int segmentId, int topologyId, long sequenceNumber) {
      return sequencers[segmentId].isNext(topologyId, sequenceNumber);
   }

   private void checkTopologyId(int topologyId) {
      if (topologyId != distributionManager.getCacheTopology().getTopologyId()) {
         throw OutdatedTopologyException.INSTANCE;
      }
   }

   private static class TriangleSequencer {
      @GuardedBy("this")
      private int senderTopologyId = -1;
      @GuardedBy("this")
      private int receiverTopologyId = -1;
      @GuardedBy("this")
      private long senderSequenceNumber = 1;
      @GuardedBy("this")
      private long receiverSequenceNumber = 1;

      private synchronized long next(int commandTopologyId) {
         if (senderTopologyId == commandTopologyId) {
            if (trace) {
               log.tracef("Sender Increment sequence (%s:%s). commandTopologyId=%s", senderTopologyId,
                     senderSequenceNumber, commandTopologyId);
            }
            return senderSequenceNumber++;
         } else if (senderTopologyId < commandTopologyId) {
            if (trace) {
               log.tracef("Sender update topology. CurrentTopologyId=%s, CommandTopologyId=%s", senderTopologyId,
                     commandTopologyId);
            }
            //update topology. this command will be the first
            senderTopologyId = commandTopologyId;
            senderSequenceNumber = 2;
            return 1;
         } else {
            if (trace) {
               log.tracef("Sender old topology. CurrentTopologyId=%s, CommandTopologyId=%s", senderTopologyId,
                     commandTopologyId);
            }
            //this topology is higher than the command topology id.
            //another topology was installed. this command will fail with OutdatedTopologyException.
            throw OutdatedTopologyException.INSTANCE;
         }
      }

      private synchronized void deliver(int commandTopologyId, long sequenceNumber) {
         if (receiverTopologyId == commandTopologyId && receiverSequenceNumber == sequenceNumber) {
            receiverSequenceNumber++;
            if (trace) {
               log.tracef("Deliver done. Next sequence (%s:%s)", receiverTopologyId, receiverSequenceNumber);
            }
         }
      }

      private synchronized boolean isNext(int commandTopologyId, long sequenceNumber) {
         if (receiverTopologyId == commandTopologyId) {
            if (trace) {
               log.tracef("Receiver old topology. Current sequence (%s:%s), command sequence (%s:%s)",
                     receiverTopologyId, receiverSequenceNumber, commandTopologyId, sequenceNumber);
            }
            return receiverSequenceNumber == sequenceNumber;
         } else if (receiverTopologyId < commandTopologyId) {
            //update topology. this command will be the first
            if (trace) {
               log.tracef("Receiver update topology. CommandTopologyId=%s, command sequence=%s", commandTopologyId,
                     sequenceNumber);
            }
            receiverTopologyId = commandTopologyId;
            receiverSequenceNumber = 1;
            return 1 == sequenceNumber;
         } else {
            if (trace) {
               log.tracef("Receiver old topology. Current sequence (%s:%s), command sequence (%s:%s)",
                     receiverTopologyId, receiverSequenceNumber, commandTopologyId, sequenceNumber);
            }
            //this topology is higher than the command topology id.
            //another topology was installed. this command will fail with OutdatedTopologyException.
            return true;
         }
      }
   }
}
