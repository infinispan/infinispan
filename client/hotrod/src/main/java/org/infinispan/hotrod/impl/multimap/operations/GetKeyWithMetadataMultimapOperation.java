package org.infinispan.hotrod.impl.multimap.operations;

import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_REQUEST;
import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_RESPONSE;
import static org.infinispan.hotrod.marshall.MarshallerUtil.bytes2obj;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.api.common.CacheEntryCollection;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.multimap.metadata.CacheEntryCollectionImpl;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import net.jcip.annotations.Immutable;

/**
 * Implements "getWithMetadata" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
@Immutable
public class GetKeyWithMetadataMultimapOperation<K, V> extends AbstractMultimapKeyOperation<K, CacheEntryCollection<K, V>> {
   private static final Log log = LogFactory.getLog(GetKeyWithMetadataMultimapOperation.class);

   public GetKeyWithMetadataMultimapOperation(OperationContext operationContext,
                                              K key, byte[] keyBytes, CacheOptions options,
                                              DataFormat dataFormat, boolean supportsDuplicates) {
      super(operationContext, GET_MULTIMAP_WITH_METADATA_REQUEST, GET_MULTIMAP_WITH_METADATA_RESPONSE, key, keyBytes, options, dataFormat, supportsDuplicates);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(new CacheEntryCollectionImpl<>(key, Collections.emptySet()));
         return;
      }
      if (!HotRodConstants.isSuccess(status)) {
         complete(null);
         return;
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
      CacheEntryExpiration expiration;
      if (lifespan < 0) {
         if (maxIdle < 0) {
            expiration = CacheEntryExpiration.IMMORTAL;
         } else {
            expiration = CacheEntryExpiration.withMaxIdle(Duration.ofSeconds(maxIdle));
         }
      } else {
         if (maxIdle < 0) {
            expiration = CacheEntryExpiration.withLifespan(Duration.ofSeconds(lifespan));
         } else {
            expiration = CacheEntryExpiration.withLifespanAndMaxIdle(Duration.ofSeconds(lifespan), Duration.ofSeconds(maxIdle));
         }
      }
      CacheEntryVersion version = new CacheEntryVersionImpl(buf.readLong());
      if (log.isTraceEnabled()) {
         log.tracef("Received version: %d", version);
      }
      int size = ByteBufUtil.readVInt(buf);
      Collection<V> values = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         V value = bytes2obj(operationContext.getChannelFactory().getMarshaller(), ByteBufUtil.readArray(buf), dataFormat().isObjectStorage(), operationContext.getConfiguration().getClassAllowList());
         values.add(value);
      }
      complete(new CacheEntryCollectionImpl<>(key, values, new CacheEntryMetadataImpl(creation, lastUsed, expiration, version)));
   }
}
