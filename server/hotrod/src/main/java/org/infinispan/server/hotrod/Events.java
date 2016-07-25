package org.infinispan.server.hotrod;

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
      protected final OperationResponse op;
      protected final byte[] listenerId;
      protected final boolean isRetried;
      protected final byte marker;

      protected Event(byte version, long messageId, OperationResponse op, byte[] listenerId, boolean isRetried, byte marker) {
         this.version = version;
         this.messageId = messageId;
         this.op = op;
         this.listenerId = listenerId;
         this.isRetried = isRetried;
         this.marker = marker;
      }

      abstract void writeEvent(ByteBuf buf);

      void defaultEvent(ByteBuf buf) {
         buf.writeByte(marker); // custom marker
         buf.writeByte(isRetried ? 1 : 0);
      }
   }

   static class KeyEvent extends Event {
      protected final byte[] key;

      protected KeyEvent(byte version, long messageId, OperationResponse op, byte[] listenerId, boolean isRetried,
                         byte[] key) {
         super(version, messageId, op, listenerId, isRetried, (byte) 0);
         this.key = key;
      }

      @Override
      public String toString() {
         final StringBuffer sb = new StringBuffer("KeyEvent{");
         sb.append("version=").append(version);
         sb.append(", messageId=").append(messageId);
         sb.append(", op=").append(op);
         sb.append(", listenerId=").append(Util.printArray(listenerId, false));
         sb.append(", isRetried=").append(isRetried);
         sb.append(", key=").append(Util.toStr(key));
         sb.append('}');
         return sb.toString();
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

      protected KeyWithVersionEvent(byte version, long messageId, OperationResponse op, byte[] listenerId, boolean isRetried,
                                    byte[] key, long dataVersion) {
         super(version, messageId, op, listenerId, isRetried, (byte) 0);
         this.key = key;
         this.dataVersion = dataVersion;
      }

      @Override
      public String toString() {
         final StringBuffer sb = new StringBuffer("KeyWithVersionEvent{");
         sb.append("version=").append(version);
         sb.append(", messageId=").append(messageId);
         sb.append(", op=").append(op);
         sb.append(", listenerId=").append(Util.printArray(listenerId, false));
         sb.append(", isRetried=").append(isRetried);
         sb.append(", key=").append(Util.toStr(key));
         sb.append(", dataVersion=").append(dataVersion);
         sb.append('}');
         return sb.toString();
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

      protected CustomEvent(byte version, long messageId, OperationResponse op, byte[] listenerId, boolean isRetried,
                            byte[] eventData) {
         super(version, messageId, op, listenerId, isRetried, (byte) 1);
         this.eventData = eventData;
      }

      @Override
      public String toString() {
         final StringBuffer sb = new StringBuffer("CustomEvent{");
         sb.append("version=").append(version);
         sb.append(", messageId=").append(messageId);
         sb.append(", op=").append(op);
         sb.append(", listenerId=").append(Util.printArray(listenerId, false));
         sb.append(", isRetried=").append(isRetried);
         sb.append(", event=").append(Util.toStr(eventData));
         sb.append('}');
         return sb.toString();
      }

      @Override
      void writeEvent(ByteBuf buf) {
         defaultEvent(buf);
         ExtendedByteBuf.writeRangedBytes(eventData, buf);
      }
   }

   static class CustomRawEvent extends Event {
      protected final byte[] eventData;

      protected CustomRawEvent(byte version, long messageId, OperationResponse op, byte[] listenerId, boolean isRetried,
                               byte[] eventData) {
         super(version, messageId, op, listenerId, isRetried, (byte) 2);
         this.eventData = eventData;
      }

      @Override
      public String toString() {
         final StringBuffer sb = new StringBuffer("CustomRawEvent{");
         sb.append("version=").append(version);
         sb.append(", messageId=").append(messageId);
         sb.append(", op=").append(op);
         sb.append(", listenerId=").append(Util.printArray(listenerId, false));
         sb.append(", isRetried=").append(isRetried);
         sb.append(", event=").append(Util.toStr(eventData));
         sb.append('}');
         return sb.toString();
      }

      @Override
      void writeEvent(ByteBuf buf) {
         defaultEvent(buf);
         ExtendedByteBuf.writeRangedBytes(eventData, buf);
      }
   }
}
