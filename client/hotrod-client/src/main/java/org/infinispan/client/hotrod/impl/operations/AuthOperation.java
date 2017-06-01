package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;

import net.jcip.annotations.Immutable;

/**
 * Performs a step in the challenge/response authentication operation
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Immutable
public class AuthOperation extends HotRodOperation {

   private final Transport transport;
   private final String saslMechanism;
   private final byte[] response;

   public AuthOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, Transport transport,
                        String saslMechanism, byte response[]) {
      super(codec, 0,  cfg, DEFAULT_CACHE_NAME_BYTES, topologyId);
      this.transport = transport;
      this.saslMechanism = saslMechanism;
      this.response = response;
   }

   @Override
   public byte[] execute() {
      HeaderParams params = writeHeader(transport, AUTH_REQUEST);
      transport.writeString(saslMechanism);
      transport.writeArray(response);
      transport.flush();

      readHeaderAndValidate(transport, params);
      boolean complete = transport.readByte() > 0;
      byte challenge[] = transport.readArray();
      return complete ? null : challenge;
   }
}
