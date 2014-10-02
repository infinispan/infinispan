package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.logging.BasicLogFactory;
import org.infinispan.commons.util.Util;
import org.jboss.logging.BasicLogger;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutIfAbsentOperation extends AbstractKeyValueOperation<byte[]> {

   private static final BasicLogger log = BasicLogFactory.getLog(PutIfAbsentOperation.class);

   public PutIfAbsentOperation(Codec codec, TransportFactory transportFactory,
                               byte[] key, byte[] cacheName, AtomicInteger topologyId,
                               Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(codec, transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
   }

   @Override
   protected byte[] executeOperation(Transport transport) {
      short status = sendPutOperation(transport, PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE);
      byte[] previousValue = null;
      if (status == NO_ERROR_STATUS || status == NOT_PUT_REMOVED_REPLACED_STATUS || status == NOT_EXECUTED_WITH_PREVIOUS) {
         previousValue = returnPossiblePrevValue(transport, status);
         if (log.isTraceEnabled()) {
            log.tracef("Returning from putIfAbsent: %s", Util.printArray(previousValue, false));
         }
      }
      return previousValue;
   }
}
