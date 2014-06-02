package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.commons.util.concurrent.NotifyingFuture;

/**
 * Performs a step in the challenge/response authentication operation
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Immutable
public class AuthOperation extends HotRodOperation<byte[]> {

   private final Transport transport;
   private final String saslMechanism;
   private final byte[] response;

   public AuthOperation(Codec codec, AtomicInteger topologyId, Transport transport, String saslMechanism, byte response[]) {
      super(codec, null, DEFAULT_CACHE_NAME_BYTES, topologyId);
      this.transport = transport;
      this.saslMechanism = saslMechanism;
      this.response = response;
   }

   @Override
   public NotifyingFuture<byte[]> executeAsync() {
      final HeaderParams params = writeHeader(transport, AUTH_REQUEST);
      transport.writeString(saslMechanism);
      transport.writeArray(response);
      return transport.flush(new Callable<byte[]>() {
         @Override
         public byte[] call() throws Exception {
            readHeaderAndValidate(transport, params);
            boolean complete = transport.readByte() > 0;
            byte challenge[] = transport.readArray();
            return complete ? null : challenge;
         }
      });
   }
}
