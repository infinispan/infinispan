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

   /**
    * The timeout in milliseconds to apply to this operation, or a value less than or equal to zero to let the
    * connection decide which configured timeout applies, see {@link #isLongRunning()}.
    *
    * @return the timeout in milliseconds
    */
   long timeout();

   /**
    * Whether this operation is expected to take considerably longer than a regular single key operation, for example
    * administration operations, bulk operations, queries, iteration or server task execution.
    * <p>
    * When no explicit {@link #timeout()} is provided, a long running operation uses
    * {@link org.infinispan.client.hotrod.configuration.Configuration#longRunningOperationTimeout()} instead of
    * {@link org.infinispan.client.hotrod.configuration.Configuration#socketTimeout()}.
    *
    * @return {@code true} if this is a long running operation
    */
   boolean isLongRunning();

   boolean isInstanceOf(Class<? extends HotRodOperation<?>> klass);

   <O extends HotRodOperation<?>> O unwrap(Class<O> klass);
}
