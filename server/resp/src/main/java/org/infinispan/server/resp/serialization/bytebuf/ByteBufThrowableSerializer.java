package org.infinispan.server.resp.serialization.bytebuf;

import java.util.function.Consumer;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.ResponseSerializer;
import org.infinispan.server.resp.serialization.ResponseWriter;

/**
 * Transform a Java throwable into a RESP3 error message.
 *
 * @author José Bolina
 */
final class ByteBufThrowableSerializer implements ResponseSerializer<Throwable, ByteBufPool> {
   static final ByteBufThrowableSerializer INSTANCE = new ByteBufThrowableSerializer();
   private static final String DEFAULT_ERROR_MESSAGE = "failed handling command";

   @Override
   public void accept(Throwable throwable, ByteBufPool alloc) {
      ByteBufResponseWriter w = new ByteBufResponseWriter(alloc);
      Consumer<ResponseWriter> writer = ResponseWriter.handleException(throwable);
      if (writer != null) {
         writer.accept(w);
      } else {
         w.error("-ERR " + extractRootCauseMessage(throwable));
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
