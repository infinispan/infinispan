package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.util.IntSet;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @since 14.0
 */
public class IterationStartOperation extends RetryOnFailureOperation<IterationStartResponse> {

   private final String filterConverterFactory;
   private final byte[][] filterParameters;
   private final IntSet segments;
   private final int batchSize;
   private final boolean metadata;
   private final SocketAddress addressTarget;
   private Channel channel;

   IterationStartOperation(OperationContext operationContext, CacheOptions options,
                           String filterConverterFactory, byte[][] filterParameters, IntSet segments,
                           int batchSize, boolean metadata, DataFormat dataFormat,
                           SocketAddress addressTarget) {
      super(operationContext, ITERATION_START_REQUEST, ITERATION_START_RESPONSE, options, dataFormat);
      this.filterConverterFactory = filterConverterFactory;
      this.filterParameters = filterParameters;
      this.segments = segments;
      this.batchSize = batchSize;
      this.metadata = metadata;
      this.addressTarget = addressTarget;
   }

   @Override
   protected void executeOperation(Channel channel) {
      this.channel = channel;
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer();

      operationContext.getCodec().writeHeader(buf, header);
      operationContext.getCodec().writeIteratorStartOperation(buf, segments, filterConverterFactory, batchSize, metadata, filterParameters);
      channel.writeAndFlush(buf);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (addressTarget != null) {
         operationContext.getChannelFactory().fetchChannelAndInvoke(addressTarget, this);
      } else {
         super.fetchChannelAndInvoke(retryCount, failedServers);
      }
   }

   public void releaseChannel(Channel channel) {
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      SegmentConsistentHash consistentHash = (SegmentConsistentHash) operationContext.getChannelFactory().getConsistentHash(operationContext.getCacheNameBytes());
      IterationStartResponse response = new IterationStartResponse(ByteBufUtil.readArray(buf), consistentHash, header.getClientTopology().get().getTopologyId(), channel);
      complete(response);
   }
}
