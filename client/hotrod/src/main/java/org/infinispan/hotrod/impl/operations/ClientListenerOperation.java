package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.api.ClientCacheListenerOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.reactivex.rxjava3.processors.FlowableProcessor;

public abstract class ClientListenerOperation<K, V> extends RetryOnFailureOperation<SocketAddress> {
   public final byte[] listenerId;
   public final ClientCacheListenerOptions.Impl listenerOptions;
   public final FlowableProcessor<CacheEntryEvent<K, V>> processor;

   // Holds which address we are currently executing the operation on
   protected SocketAddress address;

   protected ClientListenerOperation(OperationContext operationContext, short requestCode, short responseCode,
         ClientCacheListenerOptions.Impl listenerOptions, byte[] listenerId, DataFormat dataFormat, FlowableProcessor<CacheEntryEvent<K, V>> processor) {
      super(operationContext, requestCode, responseCode, listenerOptions, dataFormat);
      this.listenerId = listenerId;
      this.listenerOptions = listenerOptions;
      this.processor = processor;
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
         processor.onComplete();
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
         complete(address);
      } else {
         // this releases the channel
         operationContext.getListenerNotifier().removeClientListener(listenerId);
         // TODO: make this error return a better name?
         completeExceptionally(HOTROD.failedToAddListener(listenerId, status));
      }
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

   public DataFormat getDataFormat() {
      return dataFormat;
   }

   abstract public ClientListenerOperation copy();
}
