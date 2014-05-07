package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Remove client listener operation. In order to avoid issues with concurrent
 * event consumption, removing client listener operation is sent in a separate
 * connection to the one used for event consumption, but it must go to the
 * same node where the listener has been added.
 *
 * @author Galder Zamarreño
 */
public class RemoveClientListenerOperation extends HotRodOperation {

   private final ClientListenerNotifier listenerNotifier;

   private final Object listener;

   protected final TransportFactory transportFactory;

   protected RemoveClientListenerOperation(Codec codec, TransportFactory transportFactory,
         byte[] cacheName, AtomicInteger topologyId, Flag[] flags,
         ClientListenerNotifier listenerNotifier, Object listener) {
      super(codec, flags, cacheName, topologyId);
      this.transportFactory = transportFactory;
      this.listenerNotifier = listenerNotifier;
      this.listener = listener;
   }

   @Override
   public Object execute() {
      byte[] listenerId = listenerNotifier.findListenerId(listener);
      if (listenerId != null) {
         SocketAddress address = listenerNotifier.findTransport(listenerId).getRemoteSocketAddress();
         Transport transport = transportFactory.getAddressTransport(address);
         try {
            HeaderParams params = writeHeader(transport, REMOVE_CLIENT_LISTENER_REQUEST);
            transport.writeArray(listenerId);
            transport.flush();
            short status = readHeaderAndValidate(transport, params);
            if (status == NO_ERROR_STATUS)
               listenerNotifier.removeClientListener(listenerId);
         } finally {
            transportFactory.releaseTransport(transport);
         }
      }

      return null;
   }
}
