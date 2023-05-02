package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.client.hotrod.counter.impl.CounterOperationFactory.COUNTER_CACHE_NAME;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.Util;
import org.infinispan.counter.exception.CounterException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A base operation class for the counter's operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
abstract class BaseCounterOperation<T> extends RetryOnFailureOperation<T> {

   private static final Log commonsLog = LogFactory.getLog(BaseCounterOperation.class, Log.class);
   private static final Charset CHARSET = StandardCharsets.UTF_8;
   private static final byte[] EMPTY_CACHE_NAME = Util.EMPTY_BYTE_ARRAY;
   private final String counterName;
   private final boolean useConsistentHash;

   BaseCounterOperation(short requestCode, short responseCode, ChannelFactory channelFactory, AtomicReference<ClientTopology> clientTopology, Configuration cfg,
                        String counterName, boolean useConsistentHash) {
      super(requestCode, responseCode, channelFactory.getNegotiatedCodec(), channelFactory, EMPTY_CACHE_NAME,
            clientTopology, 0, cfg, null, null);
      this.counterName = counterName;
      this.useConsistentHash = useConsistentHash;
   }

   /**
    * Writes the operation header followed by the counter's name.
    */
   void sendHeaderAndCounterNameAndRead(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, 0);
      channel.writeAndFlush(buf);
   }

   ByteBuf getHeaderAndCounterNameBufferAndRead(Channel channel, int extraBytes) {
      scheduleRead(channel);

      // counterName should never be null/empty
      byte[] counterBytes = counterName.getBytes(HotRodConstants.HOTROD_STRING_CHARSET);
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(counterBytes) + extraBytes);
      codec.writeHeader(buf, header);
      ByteBufUtil.writeString(buf, counterName);

      setCacheName();
      return buf;
   }

   void writeHeaderAndCounterName(ByteBuf buf) {
      codec.writeHeader(buf, header);
      ByteBufUtil.writeString(buf, counterName);

      setCacheName();
   }

   /**
    * If the status is {@link #KEY_DOES_NOT_EXIST_STATUS}, the counter is undefined and a {@link CounterException} is
    * thrown.
    */
   void checkStatus(short status) {
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         throw commonsLog.undefinedCounter(counterName);
      }
   }

   void setCacheName() {
      header.cacheName(COUNTER_CACHE_NAME);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (retryCount == 0 && useConsistentHash) {
         channelFactory.fetchChannelAndInvoke(new ByteString(counterName), failedServers, COUNTER_CACHE_NAME, this);
      } else {
         channelFactory.fetchChannelAndInvoke(failedServers, COUNTER_CACHE_NAME, this);
      }
   }

   @Override
   protected Throwable handleException(Throwable cause, Channel channel, SocketAddress address) {
      cause =  super.handleException(cause, channel, address);
      if (cause instanceof CounterException) {
         completeExceptionally(cause);
         return null;
      }
      return cause;
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append("counter=").append(counterName);
   }

   private static class ByteString {

      private final int hash;
      private final byte[] b;

      private ByteString(String s) {
         //copied from ByteString in core
         this.b = s.getBytes(CHARSET);
         this.hash = Arrays.hashCode(b);
      }

      @Override
      public int hashCode() {
         return hash;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ByteString that = (ByteString) o;
         return Arrays.equals(b, that.b);
      }

      @Override
      public String toString() {
         return new String(b, CHARSET);
      }
   }
}
