package org.infinispan.server.resp;

import org.infinispan.server.resp.response.LCSResponse;
import org.infinispan.server.resp.response.SetResponse;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;
import static org.infinispan.server.resp.RespConstants.OK;
import static org.infinispan.server.resp.RespConstants.QUEUED_REPLY;

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

   public static final BiConsumer<Collection<Long>, ByteBufPool> COLLECTION_LONG_BICONSUMER = Resp3Handler::handleCollectionLongResult;

   public static final BiConsumer<Collection<Double>, ByteBufPool> COLLECTION_DOUBLE_BICONSUMER = Resp3Handler::handleCollectionDoubleResult;

   public static final BiConsumer<Double, ByteBufPool> DOUBLE_BICONSUMER = Resp3Handler::handleDoubleResult;

   public static final BiConsumer<byte[], ByteBufPool> BULK_BICONSUMER = Resp3Handler::handleBulkResult;

   public static final BiConsumer<Collection<byte[]>, ByteBufPool> COLLECTION_BULK_BICONSUMER = Resp3Handler::handleCollectionBulkResult;

   public static final BiConsumer<byte[], ByteBufPool> GET_BICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         ByteBufferUtils.bytesToResult(innerValueBytes, alloc);
      } else {
         ByteBufferUtils.stringToByteBufAscii("$-1\r\n", alloc);
      }
   };

   public static final BiConsumer<Collection<byte[]>, ByteBufPool> GET_ARRAY_BICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         ByteBufferUtils.bytesToResult(innerValueBytes, alloc);
      } else {
         ByteBufferUtils.stringToByteBufAscii("$-1\r\n", alloc);
      }
   };

   public static final BiConsumer<byte[], ByteBufPool> DELETE_BICONSUMER = (prev, alloc) ->
         ByteBufferUtils.stringToByteBufAscii(":" + (prev == null ? "0" : "1") + CRLF_STRING, alloc);

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
