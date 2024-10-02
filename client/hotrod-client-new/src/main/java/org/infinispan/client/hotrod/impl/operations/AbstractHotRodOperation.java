package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

// Extends CompletableFuture to reduce allocation size by not needed additional object header info
public abstract class AbstractHotRodOperation<T> extends CompletableFuture<T> implements HotRodConstants, HotRodOperation<T> {
   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      // Do nothing by default
   }

   @Override
   public long timeout() {
      return -1;
   }

   @Override
   public Object getRoutingObject() {
      return null;
   }

   @Override
   public boolean supportRetry() {
      return true;
   }

   @Override
   public Map<String, byte[]> additionalParameters() {
      return null;
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, T responseValue) {
      // Default is operation does nothing
   }

   @Override
   public String toString() {
      String cn = getCacheName() == null || getCacheName().isEmpty() ? "(default)" : getCacheName();
      StringBuilder sb = new StringBuilder(64);
      sb.append(getClass().getSimpleName()).append('{').append(cn);
      addParams(sb);
      sb.append(", flags=").append(Integer.toHexString(flags()));
      sb.append('}');

      sb.append("- CompletableFuture[");
      sb.append(super.toString());
      sb.append("]");

      return sb.toString();
   }

   protected void addParams(StringBuilder sb) { }

   @Override
   public void reset() { }

   @Override
   public CompletableFuture<T> asCompletableFuture() {
      return this;
   }
}
