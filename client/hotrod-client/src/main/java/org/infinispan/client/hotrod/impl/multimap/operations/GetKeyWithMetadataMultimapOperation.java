package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_RESPONSE;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.metadata.MetadataCollectionImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.multimap.MetadataCollection;

import net.jcip.annotations.Immutable;

/**
 * Implements "getWithMetadata" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class GetKeyWithMetadataMultimapOperation<V> extends AbstractMultimapKeyOperation<MetadataCollection<V>> {

   private static final Log log = LogFactory.getLog(GetKeyWithMetadataMultimapOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   public GetKeyWithMetadataMultimapOperation(Codec codec, TransportFactory transportFactory,
                                              Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags,
                                              Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected MetadataCollection<V> executeOperation(Transport transport) {
      short status = sendKeyOperation(keyBytes, transport, GET_MULTIMAP_WITH_METADATA_REQUEST, GET_MULTIMAP_WITH_METADATA_RESPONSE);

      MetadataCollection<V> result = null;
      if (HotRodConstants.isNotExist(status)) {
         result = new MetadataCollectionImpl<>(Collections.emptySet());
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
         int size = transport.readVInt();
         Collection<V> values = new ArrayList<>(size);
         for (int i = 0; i < size; ++i) {
            V value = codec.readUnmarshallByteArray(transport, status, cfg.serialWhitelist());
            values.add(value);
         }
         result = new MetadataCollectionImpl<>(values, creation, lifespan, lastUsed, maxIdle, version);
      }

      return result;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(key, failedServers, cacheName);
   }
}
