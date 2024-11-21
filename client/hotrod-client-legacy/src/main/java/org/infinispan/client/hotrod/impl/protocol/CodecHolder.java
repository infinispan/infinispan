package org.infinispan.client.hotrod.impl.protocol;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Holds a reference to the {@link Codec} negotiated with the server.
 *
 * @since 15.0
 */
public class CodecHolder {

   private static final Log log = LogFactory.getLog(CodecHolder.class);
   private final AtomicReference<Codec> codec;

   public CodecHolder(Codec configuredCodec) {
      codec = new AtomicReference<>(configuredCodec);
   }

   public Codec getCodec() {
      return codec.get();
   }

   public void setCodec(Codec newCodec) {
      Codec current = codec.get();
      if (current.equals(newCodec)) {
         return;
      }
      log.debugf("Changing codec from %s to %s", current, newCodec);
      // if multiple PING happens in parallel, at least one will succeed
      // we assume all PING return the same negotiated codec.
      codec.compareAndSet(current, newCodec);
   }
}
