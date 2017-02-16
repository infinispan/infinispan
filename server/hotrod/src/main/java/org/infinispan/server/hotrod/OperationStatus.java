package org.infinispan.server.hotrod;

import java.util.HashMap;
import java.util.Map;

/**
 * Hot Rod operation possible status outcomes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public enum OperationStatus {
   // TODO: need to get codes
   Success(0x00),
   OperationNotExecuted(0x01),
   KeyDoesNotExist(0x02),
   SuccessWithPrevious(0x03),
   NotExecutedWithPrevious(0x04),
   InvalidIteration(0x05),

   SuccessCompat(0x06),
   SuccessWithPreviousCompat(0x07),
   NotExecutedWithPreviousCompat(0x08),

   InvalidMagicOrMsgId(0x81),
   UnknownOperation(0x82),
   UnknownVersion(0x83), // todo: test
   ParseError(0x84), // todo: test
   ServerError(0x85), // todo: test
   OperationTimedOut(0x86), // todo: test
   NodeSuspected(0x87),
   IllegalLifecycleState(0x88),;

   private final static Map<Byte, OperationStatus> intMap = new HashMap<>();

   static {
      for (OperationStatus status : OperationStatus.values()) {
         intMap.put(status.code, status);
      }
   }

   private final byte code;

   OperationStatus(int code) {
      this.code = (byte) code;
   }

   public byte getCode() {
      return code;
   }

   public static OperationStatus fromCode(byte code) {
      return intMap.get(code);
   }

   static OperationStatus withCompatibility(OperationStatus st, boolean isCompatibilityEnabled) {
      if (isCompatibilityEnabled) {
         switch (st) {
            case Success:
               return SuccessCompat;
            case SuccessWithPrevious:
               return SuccessWithPreviousCompat;
            case NotExecutedWithPrevious:
               return NotExecutedWithPreviousCompat;
            default:
               return st;
         }
      } else return st;
   }
}
