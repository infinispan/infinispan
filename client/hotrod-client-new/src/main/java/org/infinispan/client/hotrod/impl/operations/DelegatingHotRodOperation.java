package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class DelegatingHotRodOperation<T> extends HotRodOperation<T> {
   protected final HotRodOperation<T> delegate;

   protected DelegatingHotRodOperation(HotRodOperation<T> delegate) {
      this.delegate = delegate;
      // TODO: maybe someday HotRodOperation won't extend CompletableFuture and we won't need this but it
      // saves an extra allocation on the happy path
      whenComplete((v, t) -> {
         if (t != null) {
            delegate.completeExceptionally(t);
         } else {
            delegate.complete(v);
         }
      });
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      delegate.writeOperationRequest(channel, buf, codec);
   }

   @Override
   public T createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return delegate.createResponse(buf, status, decoder, codec, unmarshaller);
   }

   @Override
   public short requestOpCode() {
      return delegate.requestOpCode();
   }

   @Override
   public short responseOpCode() {
      return delegate.responseOpCode();
   }

   @Override
   public int flags() {
      return delegate.flags();
   }

   @Override
   public byte[] getCacheNameBytes() {
      return delegate.getCacheNameBytes();
   }

   @Override
   public String getCacheName() {
      return delegate.getCacheName();
   }

   @Override
   public DataFormat getDataFormat() {
      return delegate.getDataFormat();
   }

   @Override
   public boolean forceSend() {
      return delegate.forceSend();
   }

   @Override
   public Object getRoutingObject() {
      return delegate.getRoutingObject();
   }

   @Override
   public boolean supportRetry() {
      return delegate.supportRetry();
   }

   @Override
   protected void addParams(StringBuilder sb) {
      delegate.addParams(sb);
   }

   @Override
   public void reset() {
      delegate.reset();
   }

   @Override
   public Map<String, byte[]> additionalParameters() {
      return delegate.additionalParameters();
   }

   @Override
   void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, T responseValue) {
      delegate.handleStatsCompletion(statistics, startTime, status, responseValue);
   }
}
