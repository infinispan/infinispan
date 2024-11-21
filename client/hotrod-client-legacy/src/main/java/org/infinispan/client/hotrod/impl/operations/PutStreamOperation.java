package org.infinispan.client.hotrod.impl.operations;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.ChannelOutputStream;
import org.infinispan.client.hotrod.impl.protocol.ChannelOutputStreamListener;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Streaming put operation
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Immutable
public class PutStreamOperation extends AbstractKeyOperation<OutputStream> implements ChannelOutputStreamListener {
   static final long VERSION_PUT = 0;
   static final long VERSION_PUT_IF_ABSENT = -1;
   private final long version;
   private final long lifespan;
   private final long maxIdle;
   private final TimeUnit lifespanTimeUnit;
   private final TimeUnit maxIdleTimeUnit;
   private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

   public PutStreamOperation(Codec codec, ChannelFactory channelFactory,
                             Object key, byte[] keyBytes, byte[] cacheName, AtomicReference<ClientTopology> clientTopology,
                             int flags, Configuration cfg, long version,
                             long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit,
                             ClientStatistics clientStatistics, TelemetryService telemetryService) {
      super(PUT_STREAM_REQUEST, PUT_STREAM_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, clientTopology,
            flags, cfg, null, clientStatistics, telemetryService);
      this.version = version;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   @Override
   public void executeOperation(Channel channel) {
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes) +
            codec.estimateExpirationSize(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit) + 8);

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      buf.writeLong(version);
      channel.writeAndFlush(buf);

      complete(new ChannelOutputStream(channel, this));
   }

   @Override
   public void releaseChannel(Channel channel) {
   }

   @Override
   public boolean completeExceptionally(Throwable ex) {
      closeFuture.completeExceptionally(ex);
      return super.completeExceptionally(ex);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status) || HotRodConstants.isNotExecuted(status) && (version != VERSION_PUT)) {
         if (HotRodConstants.isSuccess(status)) {
            statsDataStore();
         }
         closeFuture.complete(null);
      } else {
         closeFuture.completeExceptionally(new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status)));
      }
   }

   @Override
   public void onError(Channel channel, Throwable error) {
      completeExceptionally(error);
   }

   @Override
   public void onClose(Channel channel) throws IOException {
      try {
         closeFuture.join();
      } catch (CompletionException e) {
         throw new IOException(e.getCause());
      } finally {
         // When the channel is closed during the operation it's already released; don't do that again
         if (channel.isActive()) {
            channelFactory.releaseChannel(channel);
         }
      }
   }
}
