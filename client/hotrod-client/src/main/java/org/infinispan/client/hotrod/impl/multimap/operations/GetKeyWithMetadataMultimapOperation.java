package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_RESPONSE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.multimap.metadata.MetadataCollectionImpl;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.multimap.MetadataCollection;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

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

   public GetKeyWithMetadataMultimapOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes,
                                              boolean supportsDuplicates) {
      super(remoteCache, keyBytes, supportsDuplicates);
   }

   @Override
   public MetadataCollection<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec,
                                               CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isNotExist(status)) {
         return new MetadataCollectionImpl<>(Collections.emptySet());
      }
      if (!HotRodConstants.isSuccess(status)) {
         return null;
      }
      short flags = buf.readByte();
      long creation = -1;
      int lifespan = -1;
      long lastUsed = -1;
      int maxIdle = -1;
      if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
         creation = buf.readLong();
         lifespan = ByteBufUtil.readVInt(buf);
      }
      if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
         lastUsed = buf.readLong();
         maxIdle = ByteBufUtil.readVInt(buf);
      }
      long version = buf.readLong();
      if (log.isTraceEnabled()) {
         log.tracef("Received version: %d", version);
      }
      int size = ByteBufUtil.readVInt(buf);
      Collection<V> values = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         V value = unmarshaller.readValue(buf);
         values.add(value);
      }
      return new MetadataCollectionImpl<>(values, creation, lifespan, lastUsed, maxIdle, version);
   }

   @Override
   public short requestOpCode() {
      return GET_MULTIMAP_WITH_METADATA_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return GET_MULTIMAP_WITH_METADATA_RESPONSE;
   }
}
