package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.jboss.logging.BasicLogger;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutIfAbsentOperation<V> extends AbstractKeyValueOperation<V> {

   private static final BasicLogger log = LogFactory.getLog(PutIfAbsentOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   public PutIfAbsentOperation(Codec codec, TransportFactory transportFactory,
                               Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                               int flags, Configuration cfg, byte[] value, long lifespan,
                               TimeUnit lifespanTimeUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value,
            lifespan, lifespanTimeUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   protected V executeOperation(Transport transport) {
      short status = sendPutOperation(transport, PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE);
      V previousValue = null;
      if (HotRodConstants.isNotExecuted(status)) {
         previousValue = returnPossiblePrevValue(transport, status);
         if (trace) {
            log.tracef("Returning from putIfAbsent: %s", previousValue);
         }
      }
      return previousValue;
   }
}
