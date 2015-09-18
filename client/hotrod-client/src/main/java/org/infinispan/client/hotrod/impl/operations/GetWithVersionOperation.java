package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.VersionedValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Corresponds to getWithVersion operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
@Deprecated
public class GetWithVersionOperation<V> extends AbstractKeyOperation<VersionedValue<V>> {

   private static final Log log = LogFactory.getLog(GetWithVersionOperation.class);

   public GetWithVersionOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected VersionedValue<V> executeOperation(Transport transport) {
      short status = sendKeyOperation(key, transport, GET_WITH_VERSION, GET_WITH_VERSION_RESPONSE);
      VersionedValue<V> result = null;
      if (HotRodConstants.isNotExist(status)) {
         result = null;
      } else if (HotRodConstants.isSuccess(status)) {
         long version = transport.readLong();
         if (log.isTraceEnabled()) {
            log.tracef("Received version: %d", version);
         }
         V value = codec.readUnmarshallByteArray(transport, status);
         result = new VersionedValueImpl<V>(version, value);
      }
      return result;
   }
}
