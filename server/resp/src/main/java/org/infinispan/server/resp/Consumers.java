package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF;

import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.response.SetResponse;

/**
 * Utility class with Consumers
 *
 * @since 15.0
 */
public final class Consumers {

   private Consumers() {

   }

   static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);

   public static final BiConsumer<Object, ByteBufPool> OK_BICONSUMER = (ignore, alloc) ->
         alloc.acquire(OK.length).writeBytes(OK);

   public static final BiConsumer<Long, ByteBufPool> LONG_BICONSUMER = Resp3Handler::handleLongResult;

   public static final BiConsumer<Double, ByteBufPool> DOUBLE_BICONSUMER = Resp3Handler::handleDoubleResult;

   public static final BiConsumer<byte[], ByteBufPool> GET_BICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         ByteBufferUtils.bytesToResult(innerValueBytes, alloc);
      } else {
         ByteBufferUtils.stringToByteBuf("$-1\r\n", alloc);
      }
   };

   public static final BiConsumer<byte[], ByteBufPool> DELETE_BICONSUMER = (prev, alloc) ->
         ByteBufferUtils.stringToByteBuf(":" + (prev == null ? "0" : "1") + CRLF, alloc);

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
}
