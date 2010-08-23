package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.BinaryVersionedValue;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Corresponds to getWithVersion operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class GetWithVersionOperation extends AbstractKeyOperation {

   private static Log log = LogFactory.getLog(GetWithVersionOperation.class);

   public GetWithVersionOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName,
                                  AtomicInteger topologyId, Flag[] flags) {
      super(transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected Object executeOperation(Transport transport) {
      short status = sendKeyOperation(key, transport, GET_WITH_VERSION, GET_WITH_VERSION_RESPONSE);
      Object result = null;
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         result = null;
      } else if (status == NO_ERROR_STATUS) {
         long version = transport.readLong();
         if (log.isTraceEnabled()) {
            log.trace("Received version: " + version);
         }
         byte[] value = transport.readArray();
         result = new BinaryVersionedValue(version, value);
      }
      return result;
   }
}
