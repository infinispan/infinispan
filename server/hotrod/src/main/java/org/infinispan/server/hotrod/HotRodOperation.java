package org.infinispan.server.hotrod;

import java.util.EnumSet;

/**
 * Enumeration defining all of the possible hotrod operations
 *
 * @author wburns
 * @since 9.0
 */
public enum HotRodOperation {
   // Puts
   PUT(0x01, 0x02, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_RETURN_PREVIOUS_VALUE, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.VALUE),
   PUT_IF_ABSENT(0x05, 0x06, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.IS_CONDITIONAL, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_RETURN_PREVIOUS_VALUE), DecoderRequirements.VALUE),
   // Replace
   REPLACE(0x07, 0x08, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.IS_CONDITIONAL, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_RETURN_PREVIOUS_VALUE), DecoderRequirements.VALUE),
   REPLACE_IF_UNMODIFIED(0x09, 0x0A, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.IS_CONDITIONAL, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_RETURN_PREVIOUS_VALUE), DecoderRequirements.VALUE),
   // Contains
   CONTAINS_KEY(0x0F, 0x10, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   // Gets
   GET(0x03, 0x04, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   GET_WITH_VERSION(0x11, 0x12, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   GET_WITH_METADATA(0x1B, 0x1C, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   // Removes
   REMOVE(0x0B, 0x0C, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_RETURN_PREVIOUS_VALUE, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   REMOVE_IF_UNMODIFIED(0x0D, 0x0E, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.IS_CONDITIONAL, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_RETURN_PREVIOUS_VALUE), DecoderRequirements.PARAMETERS),

   // Operation(s) that end after Header is read
   PING(0x17, 0x18, EnumSet.noneOf(OpReqs.class), DecoderRequirements.HEADER),
   STATS(0x15, 0x16, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER),
   CLEAR(0x13, 0x14, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER),
   SIZE(0x29, 0x2A, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER),
   AUTH_MECH_LIST(0x21, 0x22, EnumSet.noneOf(OpReqs.class), DecoderRequirements.HEADER),

   // Operation(s) that end after Custom Header is read
   AUTH(0x23, 0x24, EnumSet.noneOf(OpReqs.class), DecoderRequirements.HEADER_CUSTOM),
   EXEC(0x2B, 0x2C, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),

   // Operations that end after a Custom Key is read
   BULK_GET(0x19, 0x1A, EnumSet.of(OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY_CUSTOM),
   BULK_GET_KEYS(0x1D, 0x1E, EnumSet.of(OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY_CUSTOM),
   QUERY(0x1F, 0x20, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.KEY_CUSTOM),
   ADD_CLIENT_LISTENER(0x25, 0x26, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.KEY_CUSTOM),
   REMOVE_CLIENT_LISTENER(0x27, 0x28, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.KEY_CUSTOM),
   ITERATION_START(0x31, 0x32, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.KEY_CUSTOM),
   ITERATION_NEXT(0x33, 0x34, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.KEY_CUSTOM),
   ITERATION_END(0x35, 0x36, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.KEY_CUSTOM),

   // Operations that end after a Custom Value is read
   PUT_ALL(0x2D, 0x2E, EnumSet.of(OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.VALUE_CUSTOM),
   GET_ALL(0x2F, 0x30, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.VALUE_CUSTOM),

   // Stream operations
   GET_STREAM(0x37, 0x38, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY_CUSTOM),
   PUT_STREAM(0x39, 0x3A, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_INDEXING, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.VALUE_CUSTOM),

   // Transaction boundaries operations
   PREPARE_TX(0x3B, 0x3C, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COMMIT_TX(0x3D, 0x3E, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   ROLLBACK_TX(0x3F, 0x40, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),

   // Responses
   ERROR(0x50),
   CACHE_ENTRY_CREATED_EVENT(0x60),
   CACHE_ENTRY_MODIFIED_EVENT(0x61),
   CACHE_ENTRY_REMOVED_EVENT(0x62),
   CACHE_ENTRY_EXPIRED_EVENT(0x63);

   private final int requestOpCode;
   private final int responseOpCode;
   private final boolean requiresKey;
   private final boolean requiresValue;
   private final DecoderRequirements decodeRequirements;
   private final boolean requiresAuthentication;
   private final boolean canSkipIndexing;
   private final boolean canSkipCacheLoading;
   private final boolean canReturnPreviousValue;
   private final boolean isConditional;
   private static final HotRodOperation REQUEST_OPCODES[];
   private static final HotRodOperation RESPONSE_OPCODES[];

   HotRodOperation(int requestOpCode, int responseOpCode, EnumSet<OpReqs> opRequirements, DecoderRequirements decoderRequirements) {
      this.requestOpCode = requestOpCode;
      this.responseOpCode = responseOpCode;
      this.decodeRequirements = decoderRequirements;
      this.requiresKey = opRequirements.contains(OpReqs.REQUIRES_KEY);
      this.requiresValue = opRequirements.contains(OpReqs.REQUIRES_VALUE);
      this.requiresAuthentication = opRequirements.contains(OpReqs.REQUIRES_AUTH);
      this.canSkipIndexing = opRequirements.contains(OpReqs.CAN_SKIP_INDEXING);
      this.canSkipCacheLoading = opRequirements.contains(OpReqs.CAN_SKIP_CACHE_LOAD);
      this.canReturnPreviousValue = opRequirements.contains(OpReqs.CAN_RETURN_PREVIOUS_VALUE);
      this.isConditional = opRequirements.contains(OpReqs.IS_CONDITIONAL);
   }

   HotRodOperation(int responseOpCode) {
      this(0, responseOpCode, EnumSet.noneOf(OpReqs.class), DecoderRequirements.HEADER);
   }

   public int getResponseOpCode() {
      return responseOpCode;
   }

   DecoderRequirements getDecoderRequirements() {
      return decodeRequirements;
   }

   boolean requiresKey() {
      return requiresKey;
   }

   boolean requireValue() {
      return requiresValue;
   }

   boolean requiresAuthentication() {
      return requiresAuthentication;
   }

   boolean canSkipIndexing() {
      return canSkipIndexing;
   }

   boolean canSkipCacheLoading() {
      return canSkipCacheLoading;
   }

   boolean isNotConditionalAndCanReturnPrevious() {
      return this == PUT;
   }

   boolean canReturnPreviousValue() {
      return canReturnPreviousValue;
   }

   boolean isConditional() {
      return isConditional;
   }

   static {
      REQUEST_OPCODES = new HotRodOperation[255];
      RESPONSE_OPCODES = new HotRodOperation[255];
      for(HotRodOperation op : HotRodOperation.values()) {
         if (op.requestOpCode > 0)
           REQUEST_OPCODES[op.requestOpCode] = op;
         if (op.responseOpCode > 0)
            RESPONSE_OPCODES[op.responseOpCode] = op;
      }
   }

   public static HotRodOperation fromRequestOpCode(byte op) {
      return REQUEST_OPCODES[op];
   }

   public static HotRodOperation fromResponseOpCode(byte op) {
      return RESPONSE_OPCODES[op];
   }
}

enum DecoderRequirements {
   HEADER,
   HEADER_CUSTOM,
   KEY,
   KEY_CUSTOM,
   PARAMETERS,
   VALUE,
   VALUE_CUSTOM
}

enum OpReqs {
   REQUIRES_KEY,
   REQUIRES_VALUE,
   REQUIRES_AUTH,
   CAN_SKIP_INDEXING,
   CAN_SKIP_CACHE_LOAD,
   CAN_RETURN_PREVIOUS_VALUE,
   IS_CONDITIONAL
}
