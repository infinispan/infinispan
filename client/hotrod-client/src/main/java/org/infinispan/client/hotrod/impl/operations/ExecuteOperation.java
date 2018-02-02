package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * ExecuteOperation.
 *
 * @author Tristan Tarrant
 * @since 7.1
 */
public class ExecuteOperation<T> extends RetryOnFailureOperation<T> {

   private final String taskName;
   private final Map<String, byte[]> marshalledParams;

   protected ExecuteOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName,
                              AtomicInteger topologyId, int flags, Configuration cfg,
                              String taskName, Map<String, byte[]> marshalledParams) {
      super(EXEC_REQUEST, EXEC_RESPONSE, codec, channelFactory, cacheName == null ? DEFAULT_CACHE_NAME_BYTES : cacheName, topologyId, flags, cfg);
      this.taskName = taskName;
      this.marshalledParams = marshalledParams;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(); // estimation too complex

      codec.writeHeader(buf, header);
      ByteBufUtil.writeString(buf, taskName);
      ByteBufUtil.writeVInt(buf, marshalledParams.size());
      for(Entry<String, byte[]> entry : marshalledParams.entrySet()) {
         ByteBufUtil.writeString(buf, entry.getKey());
         ByteBufUtil.writeArray(buf, entry.getValue());
      }
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(codec.readUnmarshallByteArray(buf, status, cfg.serialWhitelist(), channelFactory.getMarshaller()));
   }
}
