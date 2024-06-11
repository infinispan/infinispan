package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

// Extends CompletableFuture to reduce allocation size by not needed additional object header info
public abstract class HotRodOperation<T> extends CompletableFuture<T> implements HotRodConstants {
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      // Do nothing by default
   }

   /**
    * Marks the operation as allowing it to be forced to be sent. That is it can be sent before authentication is
    * complete and will immediately be sent and not enqueued.
    * @return whether the command is forcibly sent without authentication or queueing.
    */

   public boolean forceSend() {
      return false;
   }

   public abstract T createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller);

   public abstract short requestOpCode();

   public abstract short responseOpCode();

   public abstract int flags();

   public abstract byte[] getCacheNameBytes();

   public abstract String getCacheName();

   public abstract DataFormat getDataFormat();

   public Object getRoutingObject() {
      return null;
   }

   public boolean supportRetry() {
      return true;
   }

   public Map<String, byte[]> additionalParameters() {
      return null;
   }

   void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, T responseValue) {
      // Default is operation does nothing
   }

                              @Override
   public String toString() {
      String cn = getCacheName() == null || getCacheName().length() == 0 ? "(default)" : getCacheName();
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

   public void reset() { }
}
