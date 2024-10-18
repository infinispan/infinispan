package org.infinispan.server.resp.serialization;

import java.util.function.Consumer;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.RespErrorUtil;

/**
 * Transform a Java throwable into a RESP3 error message.
 *
 * @author José Bolina
 */
final class ThrowableSerializer implements ResponseSerializer<Throwable> {
   static final ThrowableSerializer INSTANCE = new ThrowableSerializer();
   private static final String DEFAULT_ERROR_MESSAGE = "failed handling command";

   @Override
   public void accept(Throwable throwable, ByteBufPool alloc) {
      Consumer<ByteBufPool> writer = RespErrorUtil.handleException(throwable);
      if (writer != null) {
         writer.accept(alloc);
      } else {
         Resp3Response.error("-ERR " + extractRootCauseMessage(throwable), alloc);
      }
   }

   private String extractRootCauseMessage(Throwable t) {
      Throwable r = t;
      while (r != null && r.getCause() != null) {
         r = r.getCause();
      }

      return r == null
            ? DEFAULT_ERROR_MESSAGE
            : r.getMessage();
   }

   @Override
   public boolean test(Object object) {
      return object instanceof Throwable;
   }
}
