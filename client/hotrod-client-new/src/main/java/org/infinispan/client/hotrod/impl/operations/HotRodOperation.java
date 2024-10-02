package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public interface HotRodOperation<T> {
   void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec);

   T createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller);

   short requestOpCode();

   short responseOpCode();

   int flags();

   byte[] getCacheNameBytes();

   String getCacheName();

   DataFormat getDataFormat();

   Object getRoutingObject();

   boolean supportRetry();

   Map<String, byte[]> additionalParameters();

   void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, T responseValue);

   void reset();

   CompletableFuture<T> asCompletableFuture();

   long timeout();
}
