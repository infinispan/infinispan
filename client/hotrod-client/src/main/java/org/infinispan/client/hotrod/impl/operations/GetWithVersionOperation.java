package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.VersionedValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
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
public class GetWithVersionOperation extends AbstractKeyOperation<VersionedValue<byte[]>> {

   private static final Log log = LogFactory.getLog(GetWithVersionOperation.class);

   public GetWithVersionOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected HeaderParams writeRequest(Transport transport) {
      return writeKeyRequest(transport, GET_WITH_VERSION);
   }

   @Override
   protected VersionedValue<byte[]> readResponse(Transport transport, HeaderParams params) {
      short status = readHeaderAndValidate(transport, params);
      VersionedValue<byte[]> result = null;
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         result = null;
      } else if (status == NO_ERROR_STATUS) {
         long version = transport.readLong();
         if (log.isTraceEnabled()) {
            log.tracef("Received version: %d", version);
         }
         byte[] value = transport.readArray();
         result = new VersionedValueImpl<byte[]>(version, value);
      }
      return result;
   }
}
