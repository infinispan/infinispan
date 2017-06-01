package org.infinispan.client.hotrod.impl.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;

import net.jcip.annotations.Immutable;

/**
 * Obtains a list of SASL authentication mechanisms supported by the server
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Immutable
public class AuthMechListOperation extends HotRodOperation {

   private final Transport transport;

   public AuthMechListOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, Transport transport) {
      super(codec, 0, cfg, DEFAULT_CACHE_NAME_BYTES, topologyId);
      this.transport = transport;
   }

   @Override
   public List<String> execute() {
      List<String> result;

      HeaderParams params = writeHeader(transport, AUTH_MECH_LIST_REQUEST);
      transport.flush();

      readHeaderAndValidate(transport, params);
      int mechCount = transport.readVInt();

      result = new ArrayList<String>();
      for (int i = 0; i < mechCount; i++) {
         result.add(transport.readString());
      }
      return result;
   }
}
