package org.infinispan.hotrod.impl.transport.handler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.configuration.ProtocolVersion;
import org.infinispan.hotrod.exceptions.InvalidResponseException;
import org.infinispan.hotrod.impl.cache.CacheEntryImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.hotrod.impl.operations.AbstractPutOperation;
import org.infinispan.hotrod.impl.operations.GetOperation;
import org.infinispan.hotrod.impl.operations.HotRodOperation;
import org.infinispan.hotrod.impl.operations.PingResponse;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HeaderParams;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

public class CacheRequestProcessor {
   private static final Log log = LogFactory.getLog(CacheRequestProcessor.class);

   protected final ChannelFactory channelFactory;
   private final HotRodConfiguration configuration;

   public CacheRequestProcessor(ChannelFactory channelFactory, HotRodConfiguration configuration) {
      this.channelFactory = channelFactory;
      this.configuration = configuration;
   }

   public void pingResponse(HotRodOperation<Object> operation, short status, short protocolVersion,
                            MediaType keyMediaType, MediaType valueMediaType, Set<Short> serverOps) {
      if (!HotRodConstants.isSuccess(status)) {
         String hexStatus = Integer.toHexString(status);
         if (log.isTraceEnabled())
            log.tracef("Unknown response status: %s", hexStatus);

         throw new InvalidResponseException("Unexpected response status: " + hexStatus);
      }

      ProtocolVersion version = ProtocolVersion.getBestVersion(protocolVersion);
      PingResponse response = new PingResponse(status, version, keyMediaType, valueMediaType, serverOps);
      if (response.getVersion() != null && configuration.version() == ProtocolVersion.PROTOCOL_VERSION_AUTO) {
         channelFactory.getCacheOperationsFactory().setCodec(Codec.forProtocol(response.getVersion()));
      }
      operation.complete(response);
   }

   public void getResponse(HotRodOperation<Object> operation, short status, byte[] data) {
      assert operation instanceof GetOperation : "Operation not get: " + operation.getClass();
      GetOperation<Object, Object> op = (GetOperation<Object, Object>) operation;
      op.statsDataRead(!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status));
      op.complete(op.dataFormat().valueToObj(data, configuration.getClassAllowList()));
   }

   public void putResponse(HotRodOperation<Object> operation, Object value, short status) {
      assert operation instanceof AbstractPutOperation : "Operation is not put: " + operation.getClass();
      AbstractPutOperation<Object, Object> op = (AbstractPutOperation) operation;
      op.statsDataStore();
      if (HotRodConstants.hasPrevious(status)) op.statsDataRead(true);
      op.complete(value);
   }

   public void topologyUpdate(HotRodOperation<?> operation, int responseTopologyId, InetSocketAddress[] addresses,
                              List<List<Integer>> segmentOwners, short hashFunctionVersion) {
      HeaderParams params = (HeaderParams) operation.header();
      SocketAddress[][] segmentOwnersArray = null;
      if (segmentOwners != null) {
         segmentOwnersArray = new SocketAddress[segmentOwners.size()][];
         for (int i = 0; i < segmentOwners.size(); i++) {
            List<Integer> ownersInSegment = segmentOwners.get(i);
            segmentOwnersArray[i] = new SocketAddress[ownersInSegment.size()];
            for (int j = 0; j < ownersInSegment.size(); j++) {
               segmentOwnersArray[i][j] = addresses[ownersInSegment.get(j)];
            }
         }
      }
      channelFactory.receiveTopology(params.cacheName(), params.getTopologyAge(), responseTopologyId, addresses,
            segmentOwnersArray, hashFunctionVersion);
   }

   public CacheEntry<?, ?> createCacheEntry(HotRodOperation<?> operation, long creation, int lifespan,
                                            long lastUsed, int maxIdle, long version, byte[] value) {
      assert operation instanceof AbstractKeyOperation : "Hot Rod operation type not accepted: " + operation.getClass();
      AbstractKeyOperation<?, ?> op = (AbstractKeyOperation<?, ?>) operation;
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
      Object v = parseToObject(operation, value);
      CacheEntryMetadata metadata = new CacheEntryMetadataImpl(creation, lastUsed, expiration, new CacheEntryVersionImpl(version));
      return new CacheEntryImpl<>(op.operationKey(), v, metadata);
   }

   public Object parseToObject(HotRodOperation<?> operation, byte[] data) {
      assert operation.header() instanceof HeaderParams : "Unknown header type";
      HeaderParams params = (HeaderParams) operation.header();
      return params.dataFormat().valueToObj(data, configuration.getClassAllowList());
   }
}
