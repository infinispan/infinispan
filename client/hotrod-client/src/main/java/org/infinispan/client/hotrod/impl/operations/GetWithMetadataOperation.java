package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
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
public class GetWithMetadataOperation<V> extends AbstractKeyOperation<MetadataValue<V>> {

   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   public GetWithMetadataOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes,
                                   byte[] cacheName, AtomicInteger topologyId, int flags,
                                   Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected MetadataValue<V> executeOperation(Transport transport) {
      short status = sendKeyOperation(keyBytes, transport, GET_WITH_METADATA, GET_WITH_METADATA_RESPONSE);
      MetadataValue<V> result = null;
      if (HotRodConstants.isNotExist(status)) {
         result = null;
      } else if (HotRodConstants.isSuccess(status)) {
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
         if (trace) {
            log.tracef("Received version: %d", version);
         }
         V value = codec.readUnmarshallByteArray(transport, status, cfg.serialWhitelist());
         result = new MetadataValueImpl<V>(creation, lifespan, lastUsed, maxIdle, version, value);
      }
      return result;
   }
}
