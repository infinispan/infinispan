package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Corresponds to getWithMetadata operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Immutable
public class GetWithMetadataOperation extends AbstractKeyOperation<MetadataValue<byte[]>> {

   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);

   public GetWithMetadataOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected MetadataValue<byte[]> executeOperation(Transport transport) {
      short status = sendKeyOperation(key, transport, GET_WITH_METADATA, GET_WITH_METADATA_RESPONSE);
      MetadataValue<byte[]> result = null;
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         result = null;
      } else if (status == NO_ERROR_STATUS) {
         short flags = transport.readByte();
         long creation = -1;
         int lifespan = -1;
         long lastUsed = -1;
         int maxIdle = -1;
         if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
            creation = transport.readLong();
            lifespan = transport.readVInt();
         }
         if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
            lastUsed = transport.readLong();
            maxIdle = transport.readVInt();
         }
         long version = transport.readLong();
         if (log.isTraceEnabled()) {
            log.tracef("Received version: %d", version);
         }
         byte[] value = transport.readArray();
         result = new MetadataValueImpl<byte[]>(creation, lifespan, lastUsed, maxIdle, version, value);
      }
      return result;
   }
}
