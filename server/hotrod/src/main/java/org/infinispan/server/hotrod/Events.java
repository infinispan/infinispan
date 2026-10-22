package org.infinispan.server.hotrod;

import java.net.InetAddress;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * @author Galder Zamarreño
 */
class Events {

   abstract static class Event {
      protected final byte version;
      protected final long messageId;
      protected final HotRodOperation op;
      protected final byte[] listenerId;
      protected final boolean isRetried;
      protected final byte marker;
      final CompletableFuture<Void> backpressureFuture;

      protected Event(byte version, long messageId, HotRodOperation op, byte[] listenerId, boolean isRetried, byte marker,
                      CompletableFuture<Void> backpressureFuture) {
         this.version = version;
         this.messageId = messageId;
         this.op = op;
         this.listenerId = listenerId;
         this.isRetried = isRetried;
         this.marker = marker;
         this.backpressureFuture = backpressureFuture;
      }

      abstract void writeEvent(ByteBuf buf);

      void defaultEvent(ByteBuf buf) {
         buf.writeByte(marker); // custom marker
         buf.writeByte(isRetried ? 1 : 0);
      }
   }

   static class KeyEvent extends Event {
      protected final byte[] key;

      protected KeyEvent(byte version, long messageId, HotRodOperation op, byte[] listenerId, boolean isRetried,
                         byte[] key, CompletableFuture<Void> backpressureFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 0, backpressureFuture);
         this.key = key;
      }

      @Override
      public String toString() {
         return "KeyEvent{" +
               "version=" + version +
               ", messageId=" + messageId +
               ", op=" + op +
               ", listenerId=" + Util.printArray(listenerId, false) +
               ", isRetried=" + isRetried +
               ", key=" + Util.toStr(key) +
               '}';
      }

      @Override
      void writeEvent(ByteBuf buf) {
         defaultEvent(buf);
         ExtendedByteBuf.writeRangedBytes(key, buf);
      }
   }

   static class KeyWithVersionEvent extends Event {
      protected final byte[] key;
      protected final long dataVersion;

      protected KeyWithVersionEvent(byte version, long messageId, HotRodOperation op, byte[] listenerId, boolean isRetried,
                                    byte[] key, long dataVersion, CompletableFuture<Void> backpressureFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 0, backpressureFuture);
         this.key = key;
         this.dataVersion = dataVersion;
      }

      @Override
      public String toString() {
         return "KeyWithVersionEvent{" +
               "version=" + version +
               ", messageId=" + messageId +
               ", op=" + op +
               ", listenerId=" + Util.printArray(listenerId, false) +
               ", isRetried=" + isRetried +
               ", key=" + Util.toStr(key) +
               ", dataVersion=" + dataVersion +
               '}';
      }

      @Override
      void writeEvent(ByteBuf buf) {
         defaultEvent(buf);
         ExtendedByteBuf.writeRangedBytes(key, buf);
         buf.writeLong(dataVersion);
      }
   }

   static class CustomEvent extends Event {
      protected final byte[] eventData;

      protected CustomEvent(byte version, long messageId, HotRodOperation op, byte[] listenerId, boolean isRetried,
                            byte[] eventData, CompletableFuture<Void> backpressureFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 1, backpressureFuture);
         this.eventData = eventData;
      }

      @Override
      public String toString() {
         return "CustomEvent{" +
               "version=" + version +
               ", messageId=" + messageId +
               ", op=" + op +
               ", listenerId=" + Util.printArray(listenerId, false) +
               ", isRetried=" + isRetried +
               ", event=" + Util.toStr(eventData) +
               '}';
      }

      @Override
      void writeEvent(ByteBuf buf) {
         defaultEvent(buf);
         ExtendedByteBuf.writeRangedBytes(eventData, buf);
      }
   }

   static class CustomRawEvent extends Event {
      protected final byte[] eventData;

      protected CustomRawEvent(byte version, long messageId, HotRodOperation op, byte[] listenerId, boolean isRetried,
                               byte[] eventData, CompletableFuture<Void> backpressureFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 2, backpressureFuture);
         this.eventData = eventData;
      }

      @Override
      public String toString() {
         return "CustomRawEvent{" +
               "version=" + version +
               ", messageId=" + messageId +
               ", op=" + op +
               ", listenerId=" + Util.printArray(listenerId, false) +
               ", isRetried=" + isRetried +
               ", event=" + Util.toStr(eventData) +
               '}';
      }

      @Override
      void writeEvent(ByteBuf buf) {
         defaultEvent(buf);
         ExtendedByteBuf.writeRangedBytes(eventData, buf);
      }
   }

   /**
    * Complete routing snapshot for a topology, together with the segments whose primary owner changed since
    * {@code baseSequence}, the sequence of the last topology the listener's connection was sent before this one.
    * A {@code baseSequence} of {@code -1} means the sender has no earlier history on this connection.
    */
   static class TopologyChangedEvent extends Event {
      private final int topologySequence;
      private final HashDistAware20Response topologyResponse;
      private final ConsistentHash ch;
      private final InetAddress localAddress;
      private final int baseSequence;
      private final BitSet changedSegments;

      protected TopologyChangedEvent(byte version, long messageId, byte[] listenerId,
                                     int topologySequence,
                                     HashDistAware20Response topologyResponse,
                                     ConsistentHash ch,
                                     InetAddress localAddress,
                                     int baseSequence,
                                     BitSet changedSegments) {
         super(version, messageId, HotRodOperation.CACHE_TOPOLOGY_CHANGED_EVENT, listenerId, false, (byte) 0, null);
         this.topologySequence = topologySequence;
         this.topologyResponse = topologyResponse;
         this.ch = ch;
         this.localAddress = localAddress;
         this.baseSequence = baseSequence;
         this.changedSegments = changedSegments;
      }

      /**
       * Replaces an older event that was never written with this one. The result carries this event's snapshot and
       * the history of both, so no primary ownership change is lost.
       */
      TopologyChangedEvent coalesce(TopologyChangedEvent older) {
         BitSet union = (BitSet) older.changedSegments.clone();
         union.or(changedSegments);
         return new TopologyChangedEvent(version, messageId, listenerId, topologySequence, topologyResponse, ch,
               localAddress, older.baseSequence, union);
      }

      @Override
      public String toString() {
         return "TopologyChangedEvent{" +
               "version=" + version +
               ", messageId=" + messageId +
               ", op=" + op +
               ", listenerId=" + Util.printArray(listenerId, false) +
               ", sequence=" + topologySequence +
               ", topologyId=" + (topologyResponse != null ? topologyResponse.topologyId : -1) +
               ", baseSequence=" + baseSequence +
               ", changedSegments=" + changedSegments +
               '}';
      }

      @Override
      void writeEvent(ByteBuf buf) {
         Map<Address, ServerAddress> members = topologyResponse.serverEndpointsMap.entrySet().stream().filter(e ->
               ch.getMembers().contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

         ExtendedByteBuf.writeUnsignedInt(topologySequence, buf);
         // History of primary ownership changes: the sequence it starts after (+1, so that 0 means unknown)
         // followed by the changed segments as a bitmap bounded by the number of segments
         ExtendedByteBuf.writeUnsignedInt(baseSequence + 1, buf);
         ExtendedByteBuf.writeRangedBytes(changedSegments.toByteArray(), buf);
         ExtendedByteBuf.writeUnsignedInt(topologyResponse.topologyId, buf); // Consistent hash identity

         // Write members
         AtomicInteger indexCount = new AtomicInteger(-1);
         ExtendedByteBuf.writeUnsignedInt(members.size(), buf);
         Map<Address, Integer> indexedMembers = new HashMap<>();
         members.forEach((addr, serverAddr) -> {
            ExtendedByteBuf.writeString(serverAddr.getHost(localAddress), buf);
            ExtendedByteBuf.writeUnsignedShort(serverAddr.getPort(), buf);
            indexCount.incrementAndGet();
            indexedMembers.put(addr, indexCount.get()); // easier indexing
         });

         // Write segment information
         int numSegments = ch.getNumSegments();
         buf.writeByte(topologyResponse.hashFunction); // Hash function
         ExtendedByteBuf.writeUnsignedInt(numSegments, buf);

         for (int segmentId = 0; segmentId < numSegments; ++segmentId) {
            List<Address> owners = ch.locateOwnersForSegment(segmentId).stream().filter(members::containsKey).collect(Collectors.toList());
            int ownersSize = owners.size();
            if (ownersSize == 0) {
               // When sending partial updates, number of owners could be 0,
               // in which case just take the first member in the list.
               buf.writeByte(1);
               ExtendedByteBuf.writeUnsignedInt(0, buf);
            } else {
               buf.writeByte(ownersSize);
               owners.forEach(ownerAddr -> {
                  Integer index = indexedMembers.get(ownerAddr);
                  if (index != null) {
                     ExtendedByteBuf.writeUnsignedInt(index, buf);
                  }
               });
            }
         }
      }
   }
}
