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

   boolean isInstanceOf(Class<? extends HotRodOperation<?>> klass);

   <O extends HotRodOperation<?>> O unwrap(Class<O> klass);

   /**
    * Invoked when a response for this operation is received and decoded from the server, but the operation's
    * {@link #asCompletableFuture() CompletableFuture} has already completed (for example, due to a client-side
    * socket timeout or cancellation).
    * <p>
    * While most operations require no action upon receiving a delayed response, stateful operations (such as
    * starting an iteration or stream) may have allocated server-side resources associated with this response.
    * Implementations can override this method to clean up those resources (e.g., sending an end operation to the
    * server or releasing retained buffers) so they are not leaked.
    * </p>
    *
    * @param responseValue the decoded response value
    * @param channel the channel on which the response was received
    */
   default void handleDelayedResponse(T responseValue, Channel channel) {
   }
}
