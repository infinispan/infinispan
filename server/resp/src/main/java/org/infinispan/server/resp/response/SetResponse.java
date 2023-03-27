package org.infinispan.server.resp.response;

public class SetResponse {

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
}
