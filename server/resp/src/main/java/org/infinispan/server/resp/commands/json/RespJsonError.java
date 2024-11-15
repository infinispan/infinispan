package org.infinispan.server.resp.commands.json;

public class RespJsonError extends RuntimeException {
   boolean mustThrow;
   public RespJsonError(String message) {
      super(message);
   }
   public RespJsonError(Throwable cause) {
      super(cause);
   }
   public RespJsonError(String message, boolean handle) {
      super(message);
      this.mustThrow = handle;
   }
   public RespJsonError(Throwable cause, boolean handle) {
      super(cause);
      this.mustThrow = handle;
   }
   public boolean mustThrow() {
      return mustThrow;
   }
}
