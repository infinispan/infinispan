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
   FORGET_TX(HotRodConstants.FORGET_TX, HotRodConstants.FORGET_TX + 1, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   FETCH_TX_RECOVERY(HotRodConstants.FETCH_TX_RECOVERY, HotRodConstants.FETCH_TX_RECOVERY + 1, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   PREPARE_TX_2(HotRodConstants.PREPARE_TX_2,HotRodConstants.PREPARE_TX_2 + 1, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),

   // Counter's operation [0x4B - 0x5F]
   COUNTER_CREATE(0x4B, 0x4C, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_GET_CONFIGURATION(0x4D, 0x4E, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_IS_DEFINED(0x4F, 0x51, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   //skip 0x50 => ERROR
   COUNTER_ADD_AND_GET(0x52, 0x53, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_RESET(0x54, 0x55, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_GET(0x56, 0x57, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_CAS(0x58, 0x59, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_ADD_LISTENER(0x5A, 0x5B, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_REMOVE_LISTENER(0x5C, 0x5D, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_REMOVE(0x5E, 0x5F, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),
   COUNTER_GET_NAMES(0x64, 0x65, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER_CUSTOM),

   // Multimap operations
   GET_MULTIMAP(0x67, 0x68, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   GET_MULTIMAP_WITH_METADATA(0x69, 0x6A, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   PUT_MULTIMAP(0x6B, 0x6C, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.VALUE),
   REMOVE_MULTIMAP(0x6D, 0x6E, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   REMOVE_ENTRY_MULTIMAP(0x6F, 0x70, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.VALUE),
   SIZE_MULTIMAP(0x71, 0x72, EnumSet.of(OpReqs.REQUIRES_AUTH), DecoderRequirements.HEADER),
   CONTAINS_ENTRY_MULTIMAP(0x73, 0x74, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.VALUE),
   CONTAINS_KEY_MULTIMAP(0x75, 0x76, EnumSet.of(OpReqs.REQUIRES_KEY, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.KEY),
   CONTAINS_VALUE_MULTIMAP(0x77, 0x78, EnumSet.of(OpReqs.REQUIRES_VALUE, OpReqs.REQUIRES_AUTH, OpReqs.CAN_SKIP_CACHE_LOAD), DecoderRequirements.VALUE),

   // 0x79 => FORGET_TX request
   // 0x7A => FORGET_TX response
   // 0x7B => FETCH_TX_RECOVERY request
   // 0x7C => FETCH_TX_RECOVERY response
   // 0x7D => PREPARE_TX_2 request
   // 0x7E => PREPARE_TX_2 response

   // Responses
   ERROR(0x50),
   CACHE_ENTRY_CREATED_EVENT(0x60),
   CACHE_ENTRY_MODIFIED_EVENT(0x61),
   CACHE_ENTRY_REMOVED_EVENT(0x62),
   CACHE_ENTRY_EXPIRED_EVENT(0x63),
   COUNTER_EVENT(0x66);

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
   public static final int REQUEST_COUNT;
   static final HotRodOperation VALUES[] = values();

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

   public int getRequestOpCode() {
      return requestOpCode;
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
      REQUEST_OPCODES = new HotRodOperation[256];
      RESPONSE_OPCODES = new HotRodOperation[256];
      int requestCount = 0;
      for(HotRodOperation op : VALUES) {
         if (op.requestOpCode > 0) {
            REQUEST_OPCODES[op.requestOpCode] = op;
            ++requestCount;
         }
         if (op.responseOpCode > 0)
            RESPONSE_OPCODES[op.responseOpCode] = op;
      }
      REQUEST_COUNT = requestCount;
   }

   public static HotRodOperation fromRequestOpCode(byte op) {
      return REQUEST_OPCODES[op & 0xff];
   }

   public static HotRodOperation fromResponseOpCode(byte op) {
      return RESPONSE_OPCODES[op & 0xff];
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
