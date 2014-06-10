package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.commons.util.Either;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.ReflectionUtil;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Galder Zamarreño
 */
public class AddClientListenerOperation extends RetryOnFailureOperation<Short> {

   private static final Log log = LogFactory.getLog(AddClientListenerOperation.class, Log.class);

   public final byte[] listenerId;

   /**
    * Decicated transport instance for adding client listener. This transport
    * is used to send events back to client and it's only released when the
    * client listener is removed.
    */
   private Transport dedicatedTransport;

   private final ClientListenerNotifier listenerNotifier;
   public final Object listener;
   public final byte[][] filterFactoryParams;
   public final byte[][] converterFactoryParams;

   protected AddClientListenerOperation(Codec codec, TransportFactory transportFactory,
         byte[] cacheName, AtomicInteger topologyId, Flag[] flags,
         ClientListenerNotifier listenerNotifier, Object listener,
         byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      super(codec, transportFactory, cacheName, topologyId, flags);
      this.listenerId = generateListenerId();
      this.listenerNotifier = listenerNotifier;
      this.listener = listener;
      this.filterFactoryParams = filterFactoryParams;
      this.converterFactoryParams = converterFactoryParams;
   }

   private byte[] generateListenerId() {
      UUID uuid = UUID.randomUUID();
      byte[] listenerId = new byte[16];
      ByteBuffer bb = ByteBuffer.wrap(listenerId);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return listenerId;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      this.dedicatedTransport = transportFactory.getTransport(failedServers);
      return dedicatedTransport;
   }

   @Override
   protected void releaseTransport(Transport transport) {
      // Do not release transport instance, it's fully dedicated to events
   }

   public Transport getDedicatedTransport() {
      return dedicatedTransport;
   }

   @Override
   protected Short executeOperation(Transport transport) {
      ClientListener clientListener = extractClientListener();

      HeaderParams params = writeHeader(transport, ADD_CLIENT_LISTENER_REQUEST);
      transport.writeArray(listenerId);

      writeNamedFactory(transport, clientListener.filterFactoryName(), filterFactoryParams);
      writeNamedFactory(transport, clientListener.converterFactoryName(), converterFactoryParams);

      transport.flush();

      listenerNotifier.addClientListener(this);
      Either<Short, ClientEvent> either;
      do {
         // Process state transfer related events or add listener response
         either = codec.readHeaderOrEvent(dedicatedTransport, params, listenerId, listenerNotifier.getMarshaller());
         switch(either.type()) {
            case LEFT:
               if (either.left() == NO_ERROR_STATUS)
                  listenerNotifier.startClientListener(listenerId);
               else // If error, remove it
                  listenerNotifier.removeClientListener(listenerId);
               break;
            case RIGHT:
               listenerNotifier.invokeEvent(listenerId, either.right());
               break;
         }
      } while (either.type() == Either.Type.RIGHT);

      return either.left();
   }

   private void writeNamedFactory(Transport transport, String factoryName, byte[][] params) {
      transport.writeString(factoryName);
      if (!factoryName.isEmpty()) {
         // A named factory was written, how many parameters?
         if (params != null) {
            transport.writeByte((short) params.length);
            for (byte[] param : params)
               transport.writeArray(param);
         } else {
            transport.writeByte((short) 0);
         }
      }
   }

   private ClientListener extractClientListener() {
      ClientListener l = ReflectionUtil.getAnnotation(listener.getClass(), ClientListener.class);
      if (l == null)
         throw log.missingClientListenerAnnotation(listener.getClass().getName());
      return l;
   }

}
