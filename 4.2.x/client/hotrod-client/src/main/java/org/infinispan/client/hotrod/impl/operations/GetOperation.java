package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "get" operation as described by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class GetOperation extends AbstractKeyOperation {

   public GetOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   public Object executeOperation(Transport transport) {
      byte[] result = null;
      short status = sendKeyOperation(key, transport, GET_REQUEST, GET_RESPONSE);
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         result = null;
      } else {
         if (status == NO_ERROR_STATUS) {
            result = transport.readArray();
         }
      }
      return result;
   }
}
