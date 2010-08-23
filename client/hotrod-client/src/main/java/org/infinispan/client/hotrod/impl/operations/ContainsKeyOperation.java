package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implemets "containsKey" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ContainsKeyOperation extends AbstractKeyOperation {

   public ContainsKeyOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected Object executeOperation(Transport transport) {
      boolean containsKey = false;
      short status = sendKeyOperation(key, transport, CONTAINS_KEY_REQUEST, CONTAINS_KEY_RESPONSE);
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         containsKey = false;
      } else if (status == NO_ERROR_STATUS) {
         containsKey = true;
      }
      return containsKey;
   }
}
