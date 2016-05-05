package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.infinispan.server.core.transport.ExtendedByteBufJava;
import scala.Enumeration;

/**
 * Companion class to Decoder2x
 *
 * @author wburns
 * @since 9.0
 */
public class Decoder2xJava {

   private static final long EXPIRATION_NONE = -1;
   private static final long EXPIRATION_DEFAULT = -2;

   private static final ExpirationParam DEFAULT_EXPIRATION = new ExpirationParam(-1, TimeUnitValue.SECONDS());

   static boolean readHeader(ByteBuf buffer, byte version, long messageId, HotRodHeader header) throws HotRodUnknownOperationException {
      if (header.op() == null) {
         int readableBytes = buffer.readableBytes();
         // We require at least 2 bytes at minimum
         if (readableBytes < 2) {
            return false;
         }
         byte streamOp = buffer.readByte();
         int length = ExtendedByteBufJava.readMaybeVInt(buffer);
         // Didn't have enough bytes for VInt or the length is too long for remaining
         if (length == Integer.MIN_VALUE || length > buffer.readableBytes()) {
            return false;
         } else if (length == 0) {
            header.cacheName_$eq("");
         } else {
            byte[] bytes = new byte[length];
            buffer.readBytes(bytes);
            header.cacheName_$eq(new String(bytes, CharsetUtil.UTF_8));
         }
         switch (streamOp) {
            case 0x01:
               header.op_$eq(HotRodOperation.PutRequest);
               break;
            case 0x03:
               header.op_$eq(HotRodOperation.GetRequest);
               break;
            case 0x05:
               header.op_$eq(HotRodOperation.PutIfAbsentRequest);
               break;
            case 0x07:
               header.op_$eq(HotRodOperation.ReplaceRequest);
               break;
            case 0x09:
               header.op_$eq(HotRodOperation.ReplaceIfUnmodifiedRequest);
               break;
            case 0x0B:
               header.op_$eq(HotRodOperation.RemoveRequest);
               break;
            case 0x0D:
               header.op_$eq(HotRodOperation.RemoveIfUnmodifiedRequest);
               break;
            case 0x0F:
               header.op_$eq(HotRodOperation.ContainsKeyRequest);
               break;
            case 0x11:
               header.op_$eq(HotRodOperation.GetWithVersionRequest);
               break;
            case 0x13:
               header.op_$eq(HotRodOperation.ClearRequest);
               break;
            case 0x15:
               header.op_$eq(HotRodOperation.StatsRequest);
               break;
            case 0x17:
               header.op_$eq(HotRodOperation.PingRequest);
               break;
            case 0x19:
               header.op_$eq(HotRodOperation.BulkGetRequest);
               break;
            case 0x1B:
               header.op_$eq(HotRodOperation.GetWithMetadataRequest);
               break;
            case 0x1D:
               header.op_$eq(HotRodOperation.BulkGetKeysRequest);
               break;
            case 0x1F:
               header.op_$eq(HotRodOperation.QueryRequest);
               break;
            case 0x21:
               header.op_$eq(HotRodOperation.AuthMechListRequest);
               break;
            case 0x23:
               header.op_$eq(HotRodOperation.AuthRequest);
               break;
            case 0x25:
               header.op_$eq(HotRodOperation.AddClientListenerRequest);
               break;
            case 0x27:
               header.op_$eq(HotRodOperation.RemoveClientListenerRequest);
               break;
            case 0x29:
               header.op_$eq(HotRodOperation.SizeRequest);
               break;
            case 0x2B:
               header.op_$eq(HotRodOperation.ExecRequest);
               break;
            case 0x2D:
               header.op_$eq(HotRodOperation.PutAllRequest);
               break;
            case 0x2F:
               header.op_$eq(HotRodOperation.GetAllRequest);
               break;
            case 0x31:
               header.op_$eq(HotRodOperation.IterationStartRequest);
               break;
            case 0x33:
               header.op_$eq(HotRodOperation.IterationNextRequest);
               break;
            case 0x35:
               header.op_$eq(HotRodOperation.IterationEndRequest);
               break;
            default:
               throw new HotRodUnknownOperationException(
                    "Unknown operation: " + streamOp, version, messageId);
         }
         buffer.markReaderIndex();
      }
      int flag = ExtendedByteBufJava.readMaybeVInt(buffer);
      if (flag == Integer.MIN_VALUE) {
         return false;
      }
      if (buffer.readableBytes() < 2) {
         return false;
      }
      byte clientIntelligence = buffer.readByte();
      int topologyId = ExtendedByteBufJava.readMaybeVInt(buffer);
      if (topologyId == Integer.MIN_VALUE) {
         return false;
      }
      header.flag_$eq(flag);
      header.clientIntel_$eq(clientIntelligence);
      header.topologyId_$eq(topologyId);

      buffer.markReaderIndex();
      return true;
   }

   /**
    * @return null if not enough bytes otherwise an optional value
    */
   static RequestParameters readParameters(HotRodHeader header, ByteBuf buffer) {
      switch (header.op()) {
         case RemoveIfUnmodifiedRequest:
            return readParameters(buffer, header, false, false, true);
         case ReplaceIfUnmodifiedRequest:
            return readParameters(buffer, header, true, true, true);
         case GetAllRequest:
            return readParameters(buffer, header, false, true, false);
         default:
            return readParameters(buffer, header, true, true, false);
      }
   }

   private static RequestParameters readParameters(ByteBuf buffer, HotRodHeader header, boolean readExpiration,
           boolean readSize, boolean readVersion) {
      ExpirationParam param1;
      ExpirationParam param2;
      long version;
      int size;

      if (readExpiration) {
         boolean pre22Version = Constants$.MODULE$.isVersionPre22(header.version());
         byte firstUnit;
         byte secondUnit;
         if (pre22Version) {
            firstUnit = secondUnit = TimeUnitValue.SECONDS();
         } else {
            if (buffer.readableBytes() == 0) {
               return null;
            }
            byte units = buffer.readByte();
            firstUnit = (byte) ((units & 0xf0) >> 4);
            secondUnit = (byte) (units & 0x0f);
         }
         param1 = readExpirationParam(pre22Version, hasFlag(header, ProtocolFlag.DefaultLifespan()), buffer, firstUnit);
         if (param1 == null) {
            return null;
         }
         param2 = readExpirationParam(pre22Version, hasFlag(header, ProtocolFlag.DefaultMaxIdle()), buffer, secondUnit);
         if (param2 == null) {
            return null;
         }
      } else {
         param1 = param2 = DEFAULT_EXPIRATION;
      }
      if (readVersion) {
         version = ExtendedByteBufJava.readUnsignedMaybeLong(buffer);
         if (version == Integer.MIN_VALUE) {
            return null;
         }
      } else {
         version = -1;
      }
      if (readSize) {
         size = ExtendedByteBufJava.readMaybeVInt(buffer);
         if (size == Integer.MIN_VALUE) {
            return null;
         }
      } else {
         size = -1;
      }
      buffer.markReaderIndex();
      return new RequestParameters(size, param1, param2, version);
   }

   private static ExpirationParam readExpirationParam(boolean pre22Version, boolean useDefault, ByteBuf buffer,
           byte timeUnit) {
      if (pre22Version) {
         int duration = ExtendedByteBufJava.readMaybeVInt(buffer);
         if (duration == Integer.MIN_VALUE) {
            return null;
         } else if (duration <= 0) {
            duration = useDefault ? (int) EXPIRATION_DEFAULT : (int) EXPIRATION_NONE;
         }
         return new ExpirationParam(duration, timeUnit);
      } else {
         switch (timeUnit) {
            // Default time unit
            case 0x07:
               return new ExpirationParam(EXPIRATION_DEFAULT, timeUnit);
            // Infinite time unit
            case 0x08:
               return new ExpirationParam(EXPIRATION_NONE, timeUnit);
            default:
               long timeDuration = ExtendedByteBufJava.readMaybeVLong(buffer);
               if (timeDuration == Long.MIN_VALUE) {
                  return null;
               }
               return new ExpirationParam(timeDuration, timeUnit);
         }
      }
   }

   private static boolean hasFlag(HotRodHeader h, Enumeration.Value f) {
      return (h.flag() & f.id()) == f.id();
   }
}
