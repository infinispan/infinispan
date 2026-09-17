package org.infinispan.client.hotrod.impl.operations;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.IntSet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationStartOperation extends AbstractCacheOperation<IterationStartResponse> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final String filterConverterFactory;
   private final byte[][] filterParameters;
   private final IntSet segments;
   private final int batchSize;
   private final boolean metadata;

   IterationStartOperation(InternalRemoteCache<?, ?> cache, String filterConverterFactory, byte[][] filterParameters, IntSet segments,
                           int batchSize, boolean metadata) {
      super(cache);
      this.filterConverterFactory = filterConverterFactory;
      this.filterParameters = filterParameters;
      this.segments = segments;
      this.batchSize = batchSize;
      this.metadata = metadata;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      codec.writeIteratorStartOperation(buf, segments, filterConverterFactory, batchSize, metadata, filterParameters);
   }

   @Override
   public IterationStartResponse createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      OperationDispatcher dispatcher = internalRemoteCache.getDispatcher();
      CacheInfo cacheInfo = dispatcher.getCacheInfo(getCacheName());
      return new IterationStartResponse(ByteBufUtil.readArray(buf), (SegmentConsistentHash) cacheInfo.getConsistentHash(),
            cacheInfo.getTopologyId(), ChannelRecord.of(decoder.getChannel()));
   }

   @Override
   public short requestOpCode() {
      return ITERATION_START_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return ITERATION_START_RESPONSE;
   }

   @Override
   public boolean supportRetry() {
      return false;
   }

   @Override
   public void handleDelayedResponse(IterationStartResponse responseValue, Channel channel) {
      byte[] iterationId;
      if (responseValue != null && (iterationId = responseValue.getIterationId()) != null && iterationId.length > 0) {
         OperationDispatcher dispatcher = internalRemoteCache.getDispatcher();
         SocketAddress socketAddress = channel != null ? ChannelRecord.of(channel) : responseValue.getSocketAddress();
         if (socketAddress != null) {
            if (log.isDebugEnabled()) {
               log.debugf("Closing iteration %s after response was received following operation completion",
                     new String(iterationId, HotRodConstants.HOTROD_STRING_CHARSET));
            }
            HotRodOperation<IterationEndResponse> endOp = internalRemoteCache.getOperationsFactory()
                  .newIterationEndOperation(iterationId);
            dispatcher.executeOnSingleAddress(endOp, socketAddress);
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Unable to close iteration %s as no dispatcher available, assuming server is gone",
                     new String(iterationId, HotRodConstants.HOTROD_STRING_CHARSET));
            }
         }
      }
   }
}
