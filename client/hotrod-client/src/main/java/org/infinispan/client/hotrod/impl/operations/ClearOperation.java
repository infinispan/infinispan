package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Corresponds to clear operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ClearOperation extends RetryOnFailureOperation {

   public ClearOperation(TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(transportFactory, cacheName, topologyId, flags);
   }

   @Override
   protected Transport getTransport(int retryCount) {
      return transportFactory.getTransport();
   }

   @Override
   protected Object executeOperation(Transport transport) {
      long messageId = writeHeader(transport, CLEAR_REQUEST);
      readHeaderAndValidate(transport, messageId, CLEAR_RESPONSE);
      return null;
   }
}
