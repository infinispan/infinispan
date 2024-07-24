package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;
import static org.infinispan.server.resp.RespConstants.NULL;
import static org.infinispan.server.resp.RespConstants.OK;
import static org.infinispan.server.resp.RespConstants.QUEUED_REPLY;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.response.LCSResponse;
import org.infinispan.server.resp.response.SetResponse;

/**
 * Utility class with Consumers
 *
 * @since 15.0
 */
public final class Consumers {

   private Consumers() {

   }

   public static final BiConsumer<Object, ByteBufPool> OK_BICONSUMER = (ignore, alloc) -> alloc.acquire(OK.length)
         .writeBytes(OK);

   public static final BiConsumer<Object, ByteBufPool> QUEUED_BICONSUMER = (ignore, alloc) -> alloc.acquire(QUEUED_REPLY.length)
         .writeBytes(QUEUED_REPLY);

   public static final BiConsumer<Long, ByteBufPool> LONG_BICONSUMER = Resp3Handler::handleLongResult;

   public static final BiConsumer<Integer, ByteBufPool> INT_BICONSUMER = (v, bp) -> LONG_BICONSUMER.accept((long) v, bp);

   public static final BiConsumer<Collection<Long>, ByteBufPool> COLLECTION_LONG_BICONSUMER = Resp3Handler::handleCollectionLongResult;

   public static final BiConsumer<Collection<Double>, ByteBufPool> COLLECTION_DOUBLE_BICONSUMER = Resp3Handler::handleCollectionDoubleResult;

   public static final BiConsumer<Double, ByteBufPool> DOUBLE_BICONSUMER = Resp3Handler::handleDoubleResult;

   public static final BiConsumer<byte[], ByteBufPool> BULK_BICONSUMER = Resp3Handler::handleBulkResult;

   public static final BiConsumer<Collection<byte[]>, ByteBufPool> COLLECTION_BULK_BICONSUMER = Resp3Handler::handleCollectionBulkResult;

   public static final BiConsumer<byte[], ByteBufPool> GET_BICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         ByteBufferUtils.bytesToResult(innerValueBytes, alloc);
      } else {
         ByteBufferUtils.stringToByteBufAscii("_\r\n", alloc);
      }
   };

   public static final BiConsumer<byte[], ByteBufPool> GET_SIMPLESTRING_BICONSUMER = (innerValueBytes, alloc) -> {
      ByteBufferUtils.stringToResult(innerValueBytes, alloc);
   };

   public static final BiConsumer<Collection<byte[]>, ByteBufPool> GET_ARRAY_BICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         ByteBufferUtils.bytesToResult(innerValueBytes, alloc);
      } else {
         alloc.acquire(NULL.length).writeBytes(NULL);
      }
   };

   public static final BiConsumer<Object, ByteBufPool> LONG_ELSE_COLLECTION = (res, alloc) -> {
      if (res instanceof Long) Consumers.LONG_BICONSUMER.accept((Long) res, alloc);
      else Consumers.GET_ARRAY_BICONSUMER.accept((Collection<byte[]>) res, alloc);
   };

   public static final BiConsumer<Collection<ScoredValue<byte[]>>, ByteBufPool> GET_OBJ_WRAPPER_ARRAY_BICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         ByteBufferUtils.bytesToResultWrapped(innerValueBytes, alloc);
      } else {
         alloc.acquire(NULL.length).writeBytes(NULL);
      }
   };

   public static final BiConsumer<byte[], ByteBufPool> DELETE_BICONSUMER = (prev, alloc) ->
         ByteBufferUtils.stringToByteBufAscii(":" + (prev == null ? "0" : "1") + CRLF_STRING, alloc);

   public static final BiConsumer<Boolean, ByteBufPool> BOOLEAN_BICONSUMER = (res, alloc) -> {
      if (res) {
         LONG_BICONSUMER.accept(1L, alloc);
      } else {
         LONG_BICONSUMER.accept(0L, alloc);
      }
   };

   public static final BiConsumer<Map<byte[], Collection<byte[]>>, ByteBufPool> MAP_CONSUMER = (res, alloc) -> {
      if (res == null) {
         alloc.acquire(NULL.length).writeBytes(NULL);
         return;
      }

      Resp3Handler.writeMapPrefix(res.size(), alloc);
      for (Map.Entry<byte[], Collection<byte[]>> entry : res.entrySet()) {
         Resp3Handler.handleBulkResult(entry.getKey(), alloc);
         Resp3Handler.handleCollectionBulkResult(entry.getValue(), alloc);
      }
   };

   public static final BiConsumer<SetResponse, ByteBufPool> SET_BICONSUMER = (res, alloc) -> {
      // The set operation has three return options, with a precedence:
      //
      // 1. Previous value or `nil`: when `GET` flag present;
      // 2. `OK`: when set operation succeeded
      // 3. `nil`: when set operation failed, e.g., tried using XX or NX.
      if (res.isReturnValue()) {
         GET_BICONSUMER.accept(res.value(), alloc);
         return;
      }

      if (res.isSuccess()) {
         OK_BICONSUMER.accept(res, alloc);
         return;
      }

      GET_BICONSUMER.accept(null, alloc);
   };

   public static final BiConsumer<LCSResponse, ByteBufPool> LCS_BICONSUMER = (res, alloc) -> {
      // If lcs present, return a bulk_string
      if (res.lcs != null) {
         Resp3Handler.handleBulkResult(res.lcs, alloc);
         return;
      }
      // If idx is null then it's a justLen command, return a long
      if (res.idx == null) {
         Resp3Handler.handleLongResult(Long.valueOf(res.len), alloc);
         return;
      }
      handleIdxArray(res, alloc);
   };

   public static final BiConsumer<List, ByteBufPool> LMPOP_BICONSUMER = (res, alloc) -> {
      Resp3Handler.writeArrayPrefix(2, alloc);
      Resp3Handler.handleBulkResult((byte[])res.get(0), alloc);
      Collection<byte[]> values = (Collection<byte[]>)res.get(1);
      Resp3Handler.writeArrayPrefix(values.size(), alloc);
      for (byte[] val : values) {
         Resp3Handler.handleBulkResult(val, alloc);
      }
   };

   public static final BiConsumer<List, ByteBufPool> ZMPOP_BICONSUMER = (res, alloc) -> {
      Resp3Handler.writeArrayPrefix(2, alloc);
      // name of the set
      Resp3Handler.handleBulkResult((byte[])res.get(0), alloc);
      // array of values
      Collection<ScoredValue<byte[]>> values = (Collection<ScoredValue<byte[]>>)res.get(1);
      Resp3Handler.writeArrayPrefix(values.size(), alloc);
      for (ScoredValue<byte[]> val : values) {
         Resp3Handler.writeArrayPrefix(2, alloc);
         Resp3Handler.handleBulkResult(val.getValue(), alloc);
         Resp3Handler.handleDoubleResult(val.score(), alloc);
      }
   };

   private static void handleIdxArray(LCSResponse res, ByteBufPool alloc) {
      // return idx. it's a 4 items array
      Resp3Handler.writeArrayPrefix(4, alloc);
      Resp3Handler.handleBulkAsciiResult("matches", alloc);
      Resp3Handler.writeArrayPrefix(res.idx.size(), alloc);
      for (var match : res.idx) {
         // 2 positions + optional length
         var size = match.length > 4 ? 3 : 2;
         Resp3Handler.writeArrayPrefix(size, alloc);
         Resp3Handler.writeArrayPrefix(2, alloc);
         Resp3Handler.handleLongResult(match[0], alloc);
         Resp3Handler.handleLongResult(match[1], alloc);
         Resp3Handler.writeArrayPrefix(2, alloc);
         Resp3Handler.handleLongResult(match[2], alloc);
         Resp3Handler.handleLongResult(match[3], alloc);
         if (size == 3) {
            Resp3Handler.handleLongResult(match[4], alloc);
         }
      }
      Resp3Handler.handleBulkAsciiResult("len", alloc);
      Resp3Handler.handleLongResult((long) res.len, alloc);
   }
}
