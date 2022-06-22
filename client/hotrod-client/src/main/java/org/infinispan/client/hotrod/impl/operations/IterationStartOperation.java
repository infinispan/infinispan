package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.util.IntSet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationStartOperation extends RetryOnFailureOperation<IterationStartResponse> {

   private final String filterConverterFactory;
   private final byte[][] filterParameters;
   private final IntSet segments;
   private final int batchSize;
   private final ChannelFactory channelFactory;
   private final boolean metadata;
   private final SocketAddress addressTarget;
   private Channel channel;

   IterationStartOperation(Codec codec, int flags, Configuration cfg, byte[] cacheName, AtomicReference<ClientTopology> clientTopology,
                           String filterConverterFactory, byte[][] filterParameters, IntSet segments,
                           int batchSize, ChannelFactory channelFactory, boolean metadata, DataFormat dataFormat,
                           SocketAddress addressTarget) {
      super(ITERATION_START_REQUEST, ITERATION_START_RESPONSE, codec, channelFactory, cacheName, clientTopology, flags, cfg,
            dataFormat, null);
      this.filterConverterFactory = filterConverterFactory;
      this.filterParameters = filterParameters;
      this.segments = segments;
      this.batchSize = batchSize;
      this.channelFactory = channelFactory;
      this.metadata = metadata;
      this.addressTarget = addressTarget;
   }

   @Override
   protected void executeOperation(Channel channel) {
      this.channel = channel;
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer();

      codec.writeHeader(buf, header);
      codec.writeIteratorStartOperation(buf, segments, filterConverterFactory, batchSize, metadata, filterParameters);
      channel.writeAndFlush(buf);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (addressTarget != null) {
         channelFactory.fetchChannelAndInvoke(addressTarget, this);
      } else {
         super.fetchChannelAndInvoke(retryCount, failedServers);
      }
   }

   public void releaseChannel(Channel channel) {
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      SegmentConsistentHash consistentHash = (SegmentConsistentHash) channelFactory.getConsistentHash(cacheName());
      IterationStartResponse response = new IterationStartResponse(ByteBufUtil.readArray(buf), consistentHash, header.getClientTopology().get().getTopologyId(), channel);
      complete(response);
   }
}
