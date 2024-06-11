package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class StatisticOperation<E> extends DelegatingHotRodOperation<E> {
   private final ClientStatistics statistics;
   private long startTime;

   protected StatisticOperation(HotRodOperation<E> delegate, ClientStatistics statistics) {
      super(delegate);
      this.statistics = statistics;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      startTime = statistics.time();
   }

   @Override
   public E createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      E responseValue = super.createResponse(buf, status, decoder, codec, unmarshaller);
      delegate.handleStatsCompletion(statistics, startTime, status, responseValue);
      return responseValue;
   }
}
