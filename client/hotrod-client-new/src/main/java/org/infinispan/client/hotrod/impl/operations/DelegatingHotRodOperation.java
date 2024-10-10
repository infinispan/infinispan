package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class DelegatingHotRodOperation<T> implements HotRodOperation<T> {
   protected final HotRodOperation<T> delegate;

   protected DelegatingHotRodOperation(HotRodOperation<T> delegate) {
      this.delegate = delegate;
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
   public Object getRoutingObject() {
      return delegate.getRoutingObject();
   }

   @Override
   public boolean supportRetry() {
      return delegate.supportRetry();
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
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, T responseValue) {
      delegate.handleStatsCompletion(statistics, startTime, status, responseValue);
   }

   @Override
   public CompletableFuture<T> asCompletableFuture() {
      return delegate.asCompletableFuture();
   }

   @Override
   public long timeout() {
      return delegate.timeout();
   }

   @Override
   public boolean isInstanceOf(Class<? extends HotRodOperation<?>> klass) {
      return delegate.isInstanceOf(klass);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "delegate=" + delegate +
            '}';
   }
}
