package org.infinispan.client.hotrod.counter.operation;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * An add listener operation for {@link StrongCounter#addListener(CounterListener)} and {@link
 * WeakCounter#addListener(CounterListener)}
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class AddListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;
   private final SocketAddress server;
   private Transport dedicatedTransport;

   public AddListenerOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName, byte[] listenerId, SocketAddress server) {
      super(codec, transportFactory, topologyId, cfg, counterName);
      this.listenerId = listenerId;
      this.server = server;
   }

   public Transport getDedicatedTransport() {
      return dedicatedTransport;
   }

   @Override
   protected Boolean executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_ADD_LISTENER_REQUEST);
      transport.writeArray(listenerId);
      transport.flush();

      short status = readHeaderAndValidateCounter(transport, header);
      if (status == NO_ERROR_STATUS) {
         dedicatedTransport = transport; //this transport will be used!
         return true;
      }
      return false;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      //we have a dedicated connection to "server". lets try to register new counter in that server!
      return server == null ?
             super.getTransport(retryCount, failedServers) :
             transportFactory.getAddressTransport(server);
   }

   @Override
   protected void releaseTransport(Transport transport) {
      if (dedicatedTransport != transport) {
         //we aren't using this transport. we can release it
         super.releaseTransport(transport);
      }
   }
}
