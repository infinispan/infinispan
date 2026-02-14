package org.infinispan.server.resp.commands.bitmap;

import static org.infinispan.server.resp.commands.ArgumentUtils.toInt;
import static org.infinispan.server.resp.commands.ArgumentUtils.toLong;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.logging.Messages;

@ProtoTypeId(ProtoStreamTypeIds.RESP_BITFIELD_OPERATION)
public record BitfieldOperation(Type type, int offset, long value, boolean signed, int bits, Overflow overflow) {

   public enum Overflow {
      NONE, WRAP, SAT, FAIL;
   }

   public enum Type {
      GET, SET, INCRBY
   }

   public static BitfieldOperation GET(byte[] encoding, byte[] offset) {
      return new BitfieldOperation(Type.GET, toOffset(offset), 0, isSigned(encoding), toBits(encoding), Overflow.NONE);
   }

   public static BitfieldOperation SET(byte[] encoding, byte[] offset, byte[] value, Overflow overflow) {
      return new BitfieldOperation(Type.SET, toOffset(offset), toLong(value), isSigned(encoding), toBits(encoding), overflow);
   }

   public static BitfieldOperation INCRBY(byte[] encoding, byte[] offset, byte[] increment, Overflow overflow) {
      return new BitfieldOperation(Type.INCRBY, toOffset(offset), toLong(increment), isSigned(encoding), toBits(encoding), overflow);
   }

   private static int toOffset(byte[] value) {
      int offset = toInt(value);
      if (offset < 0) {
         throw new IllegalArgumentException(Messages.MESSAGES.invalidBitOffset());
      }
      return offset;
   }

   private static int toBits(byte[] encoding) {
      boolean signed = isSigned(encoding);
      int bits = toInt(encoding, 1);
      if (bits < 1 || (signed && bits > 64) || (!signed && bits > 63)) {
         throw new IllegalArgumentException(Messages.MESSAGES.invalidBitfieldType());
      }
      return bits;
   }

   private static boolean isSigned(byte[] type) {
      if (type[0] == 'i') {
         return true;
      } else if (type[0] == 'u') {
         return false;
      } else {
         throw new IllegalArgumentException(Messages.MESSAGES.invalidBitfieldType());
      }
   }

   public Long apply(AtomicReference<byte[]> valueRef) {
      int byteIndex = offset / 8;
      int bitIndex = offset % 8;
      int byteSize = (bits + 7) / 8;
      byte[] value = valueRef.get();

      if (byteIndex + byteSize > value.length) {
         value = Arrays.copyOf(value, byteIndex + byteSize);
         valueRef.set(value);
      }
      return switch (type()) {
         case GET -> get(value, byteIndex, bitIndex);
         case SET -> {
            long result = get(value, byteIndex, bitIndex);
            set(value, byteIndex, bitIndex, value());
            yield result;
         }
         case INCRBY -> {
            long result = get(value, byteIndex, bitIndex) + value();
            yield set(value, byteIndex, bitIndex, result);
         }
      };
   }

   private Long get(byte[] value, int byteIndex, int bitIndex) {
      long result = 0;
      for (int i = 0; i < bits; i++) {
         int currentBit = bitIndex + i;
         int currentByte = byteIndex + currentBit / 8;
         int currentBitInByte = currentBit % 8;
         if ((value[currentByte] & (1 << (7 - currentBitInByte))) != 0) {
            result |= (1L << (bits - 1 - i));
         }
      }
      if (signed && (result & (1L << (bits - 1))) != 0) {
         result |= (-1L << bits);
      }
      return result;
   }

   private Long set(byte[] value, int byteIndex, int bitIndex, long val) {
      if (overflow != BitfieldOperation.Overflow.NONE) {
         long maxVal = (1L << (bits - (signed ? 1 : 0))) - 1;
         switch (overflow) {
            case WRAP:
               val &= maxVal;
               break;
            case SAT:
               if (val > maxVal) {
                  val = maxVal;
               } else if (val < -maxVal - 1) {
                  val = -maxVal - 1;
               }
               break;
            case FAIL:
               if (val > maxVal || val < 0) {
                  return null;
               }
               break;
         }
      }

      for (int i = 0; i < bits; i++) {
         int currentBit = bitIndex + i;
         int currentByte = byteIndex + currentBit / 8;
         int currentBitInByte = currentBit % 8;
         if ((val & (1L << (bits - 1 - i))) != 0) {
            value[currentByte] |= (byte) (1 << (7 - currentBitInByte));
         } else {
            value[currentByte] &= (byte) ~(1 << (7 - currentBitInByte));
         }
      }
      return val;
   }

   public static List<Long> apply(List<BitfieldOperation> operations, AtomicReference<byte[]> value) {
      return operations.stream().map(operation -> operation.apply(value)).toList();
   }
}
