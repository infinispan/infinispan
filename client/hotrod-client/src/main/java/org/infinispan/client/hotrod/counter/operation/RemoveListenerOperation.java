package org.infinispan.client.hotrod.counter.operation;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.Handle;

/**
 * A remove listener operation for {@link Handle#remove()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;
   private final SocketAddress server;

   public RemoveListenerOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName, byte[] listenerId, SocketAddress server) {
      super(codec, transportFactory, topologyId, cfg, counterName);
      this.listenerId = listenerId;
      this.server = server;
   }


   @Override
   protected Boolean executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_ADD_LISTENER_REQUEST);
      transport.writeArray(listenerId);
      transport.flush();

      short status = readHeaderAndValidateCounter(transport, header);
      return status == NO_ERROR_STATUS;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return server == null ?
             super.getTransport(retryCount, failedServers) :
             transportFactory.getAddressTransport(server);
   }
}
