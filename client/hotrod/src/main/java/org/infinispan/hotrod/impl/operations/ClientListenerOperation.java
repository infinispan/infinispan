package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class ClientListenerOperation extends RetryOnFailureOperation<SocketAddress> {
   public final byte[] listenerId;
   public final Object listener;

   // Holds which address we are currently executing the operation on
   protected SocketAddress address;

   protected ClientListenerOperation(OperationContext operationContext, short requestCode, short responseCode,
                                     CacheOptions options, byte[] listenerId, DataFormat dataFormat, Object listener) {
      super(operationContext, requestCode, responseCode, options, dataFormat);
      this.listenerId = listenerId;
      this.listener = listener;
   }

   protected static byte[] generateListenerId() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      byte[] listenerId = new byte[16];
      ByteBuffer bb = ByteBuffer.wrap(listenerId);
      bb.putLong(random.nextLong());
      bb.putLong(random.nextLong());
      return listenerId;
   }

   public String getCacheName() {
      return operationContext.getCacheName();
   }

   @Override
   protected final void executeOperation(Channel channel) {
      // Note: since the HeaderDecoder now supports decoding both operations and events we don't have to
      // wait until all operations complete; the server will deliver responses and we'll just handle them regardless
      // of the order
      if (!channel.isActive()) {
         channelInactive(channel);
         return;
      }
      this.address = ChannelRecord.of(channel).getUnresolvedAddress();
      actualExecute(channel);
   }

   protected abstract void actualExecute(Channel channel);

   protected void cleanup(Channel channel) {
      channel.eventLoop().execute(() -> {
         if (!operationContext.getCodec().allowOperationsAndEvents()) {
            if (channel.isOpen()) {
               operationContext.getChannelFactory().releaseChannel(channel);
            }
         }
         HeaderDecoder decoder = channel.pipeline().get(HeaderDecoder.class);
         if (decoder != null) {
            decoder.removeListener(listenerId);
         }
      });
   }

   @Override
   public void releaseChannel(Channel channel) {
      if (operationContext.getCodec().allowOperationsAndEvents()) {
         super.releaseChannel(channel);
      }
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status)) {
         decoder.addListener(listenerId);
         operationContext.getListenerNotifier().startClientListener(listenerId);
      } else {
         // this releases the channel
         operationContext.getListenerNotifier().removeClientListener(listenerId);
         throw HOTROD.failedToAddListener(listener, status);
      }
      complete(address);
   }

   @Override
   public boolean completeExceptionally(Throwable ex) {
      if (!isDone()) {
         operationContext.getListenerNotifier().removeClientListener(listenerId);
      }
      return super.completeExceptionally(ex);
   }

   public void postponeTimeout(Channel channel) {
      assert !isDone();
      timeoutFuture.cancel(false);
      timeoutFuture = null;
      scheduleTimeout(channel);
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append("listenerId=").append(Util.printArray(listenerId));
   }

   abstract public ClientListenerOperation copy();
}
