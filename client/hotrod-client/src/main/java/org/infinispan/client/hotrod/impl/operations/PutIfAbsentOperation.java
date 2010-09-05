package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutIfAbsentOperation extends AbstractKeyValueOperation {

   private static final Log log = LogFactory.getLog(PutIfAbsentOperation.class);

   public PutIfAbsentOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName, AtomicInteger topologyId,
                               Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
   }

   @Override
   protected Object executeOperation(Transport transport) {
      short status = sendPutOperation(transport, PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE);
      byte[] previousValue = null;
      if (status == NO_ERROR_STATUS || status == NOT_PUT_REMOVED_REPLACED_STATUS) {
         previousValue = returnPossiblePrevValue(transport);
         if (log.isTraceEnabled()) {
            log.trace("Returning from putIfAbsent: " + Util.printArray(previousValue, false));
         }
      }
      return previousValue;
   }
}
