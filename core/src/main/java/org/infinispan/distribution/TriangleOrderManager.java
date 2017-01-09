package org.infinispan.distribution;

import java.util.Iterator;
import java.util.Map;

import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;

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
public class TriangleOrderManager {

   private final TriangleSequencer[] sequencers;
   private volatile CacheTopology currentCacheTopology;

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

   public void next(Map<Integer, Long> segmentsIds, final int commandTopologyId) {
      checkTopologyId(commandTopologyId);
      try {
         for (Map.Entry<Integer, Long> entry : segmentsIds.entrySet()) {
            entry.setValue(getNext(entry.getKey(), commandTopologyId));
         }
      } finally {
         //check if topology didn't change in the meanwhile
         checkTopologyId(commandTopologyId);
      }
   }

   public boolean isNext(int segmentId, long sequenceNumber, int commandTopologyId) {
      final int topologyId = currentCacheTopology.getTopologyId();
      return commandTopologyId < topologyId ||
            (commandTopologyId == topologyId && checkIfNext(segmentId, commandTopologyId, sequenceNumber));
   }

   public boolean isNext(Map<Integer, Long> sequenceNumbers, int commandTopologyId) {
      final int topologyId = currentCacheTopology.getTopologyId();
      return commandTopologyId < topologyId ||
            (commandTopologyId == topologyId && checkAllEntries(sequenceNumbers, commandTopologyId));
   }

   public void markDelivered(int segmentId, long sequenceNumber, int commandTopologyId) {
      sequencers[segmentId].deliver(commandTopologyId, sequenceNumber);
   }

   public void updateCacheTopology(CacheTopology newCacheTopology) {
      this.currentCacheTopology = newCacheTopology;
   }

   private long getNext(int segmentId, int topologyId) {
      return sequencers[segmentId].next(topologyId);
   }

   private boolean checkIfNext(int segmentId, int topologyId, long sequenceNumber) {
      return sequencers[segmentId].isNext(topologyId, sequenceNumber);
   }

   private boolean checkAllEntries(Map<Integer, Long> sequenceNumbers, int commandTopologyId) {
      Iterator<Map.Entry<Integer, Long>> iterator = sequenceNumbers.entrySet().iterator();
      boolean result = true;
      while (result && iterator.hasNext()) {
         Map.Entry<Integer, Long> entry = iterator.next();
         result = checkIfNext(entry.getKey(), commandTopologyId, entry.getValue());
      }
      return result;
   }

   private void checkTopologyId(int topologyId) {
      if (topologyId != currentCacheTopology.getTopologyId()) {
         throw OutdatedTopologyException.getCachedInstance();
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
            return senderSequenceNumber++;
         } else if (senderTopologyId < commandTopologyId) {
            //update topology. this command will be the first
            senderTopologyId = commandTopologyId;
            senderSequenceNumber = 2;
            return 1;
         } else {
            //this topology is higher than the command topology id.
            //another topology was installed. this command will fail with OutdatedTopologyException.
            throw OutdatedTopologyException.getCachedInstance();
         }
      }

      private synchronized void deliver(int commandTopologyId, long sequenceNumber) {
         if (receiverTopologyId == commandTopologyId && receiverSequenceNumber == sequenceNumber) {
            receiverSequenceNumber++;
         }
      }

      private synchronized boolean isNext(int commandTopologyId, long sequenceNumber) {
         if (receiverTopologyId == commandTopologyId) {
            return receiverSequenceNumber == sequenceNumber;
         } else if (receiverTopologyId < commandTopologyId) {
            //update topology. this command will be the first
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
