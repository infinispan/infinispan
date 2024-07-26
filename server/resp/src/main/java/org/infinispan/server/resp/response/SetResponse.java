package org.infinispan.server.resp.response;

import java.util.function.BiConsumer;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;

public class SetResponse {

   public static final BiConsumer<SetResponse, ByteBufPool> SERIALIZER = (res, alloc) ->
         Resp3Response.write(res, alloc, SetResponseSerializer.INSTANCE);

   private final byte[] value;
   private final boolean returnValue;
   private final boolean success;

   public SetResponse(byte[] value, boolean returnValue) {
      this(value, returnValue, true);
   }

   public SetResponse(byte[] value, boolean returnValue, boolean success) {
      this.value = value;
      this.returnValue = returnValue;
      this.success = success;
   }

   public byte[] value() {
      return value;
   }

   public boolean isReturnValue() {
      return returnValue;
   }

   public boolean isSuccess() {
      return success;
   }

   private static final class SetResponseSerializer implements JavaObjectSerializer<SetResponse> {
      private static final SetResponseSerializer INSTANCE = new SetResponseSerializer();

      @Override
      public void accept(SetResponse res, ByteBufPool alloc) {
         // The set operation has three return options, with a precedence:
         //
         // 1. Previous value or `nil`: when `GET` flag present;
         // 2. `OK`: when set operation succeeded
         // 3. `nil`: when set operation failed, e.g., tried using XX or NX.
         if (res.isReturnValue()) {
            Resp3Response.string(res.value(), alloc);
            return;
         }

         if (res.isSuccess()) {
            Resp3Response.ok(alloc);
            return;
         }

         Resp3Response.nulls(alloc);
      }
   }
}
