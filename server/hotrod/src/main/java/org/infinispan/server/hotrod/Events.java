package org.infinispan.server.hotrod;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.util.Util;
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

      // Delays the operation that generated this event
      protected final CompletableFuture<Void> eventFuture;

      protected Event(byte version, long messageId, HotRodOperation op, byte[] listenerId, boolean isRetried, byte marker,
                      CompletableFuture<Void> eventFuture) {
         this.version = version;
         this.messageId = messageId;
         this.op = op;
         this.listenerId = listenerId;
         this.isRetried = isRetried;
         this.marker = marker;
         this.eventFuture = eventFuture;
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
                         byte[] key, CompletableFuture<Void> eventFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 0, eventFuture);
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
                                    byte[] key, long dataVersion, CompletableFuture<Void> eventFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 0, eventFuture);
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
                            byte[] eventData, CompletableFuture<Void> eventFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 1, eventFuture);
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
                               byte[] eventData, CompletableFuture<Void> eventFuture) {
         super(version, messageId, op, listenerId, isRetried, (byte) 2, eventFuture);
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
}
