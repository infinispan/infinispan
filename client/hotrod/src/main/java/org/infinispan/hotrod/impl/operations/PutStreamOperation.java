package org.infinispan.hotrod.impl.operations;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.exceptions.InvalidResponseException;
import org.infinispan.hotrod.impl.protocol.ChannelOutputStream;
import org.infinispan.hotrod.impl.protocol.ChannelOutputStreamListener;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Streaming put operation
 *
 * @since 14.0
 */
public class PutStreamOperation<K> extends AbstractKeyOperation<K, OutputStream> implements ChannelOutputStreamListener {
   static final long VERSION_PUT = 0;
   static final long VERSION_PUT_IF_ABSENT = -1;
   private final long version;
   private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

   public PutStreamOperation(OperationContext operationContext,
                             K key, byte[] keyBytes,
                             CacheWriteOptions options, long version) {
      super(operationContext, PUT_STREAM_REQUEST, PUT_STREAM_RESPONSE, key, keyBytes, options, null);
      this.version = version;
   }

   @Override
   public void executeOperation(Channel channel) {
      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      CacheEntryExpiration.Impl expiration = (CacheEntryExpiration.Impl) ((CacheWriteOptions) options).expiration();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes) +
            codec.estimateExpirationSize(expiration) + 8);

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      codec.writeExpirationParams(buf, expiration);
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
            operationContext.getChannelFactory().releaseChannel(channel);
         }
      }
   }
}
