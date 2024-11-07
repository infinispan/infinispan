package org.infinispan.server.resp.response;

import java.util.function.BiConsumer;

import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.ResponseWriter;

public class SetResponse {

   public static final BiConsumer<SetResponse, ResponseWriter> SERIALIZER = (res, writer) ->
         writer.write(res, SetResponseSerializer.INSTANCE);

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
      public void accept(SetResponse res, ResponseWriter writer) {
         // The set operation has three return options, with a precedence:
         //
         // 1. Previous value or `nil`: when `GET` flag present;
         // 2. `OK`: when set operation succeeded
         // 3. `nil`: when set operation failed, e.g., tried using XX or NX.
         if (res.isReturnValue()) {
            writer.string(res.value());
            return;
         }

         if (res.isSuccess()) {
            writer.ok();
            return;
         }

         writer.nulls();
      }
   }
}
