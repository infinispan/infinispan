package org.infinispan.client.hotrod.counter.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation for {@link CounterManager#getCounterNames()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetCounterNamesOperation extends BaseCounterOperation<Collection<String>> {
   private int size;
   private Collection<String> names;

   public GetCounterNamesOperation(Codec codec, ChannelFactory transportFactory, AtomicInteger topologyId,
                                   Configuration cfg) {
      super(COUNTER_GET_NAMES_REQUEST, COUNTER_GET_NAMES_RESPONSE, codec, transportFactory, topologyId, cfg, "");
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendHeader(channel);
      setCacheName();
   }

   @Override
   protected void reset() {
      super.reset();
      names = null;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      assert status == NO_ERROR_STATUS;
      if (names == null) {
         size = ByteBufUtil.readVInt(buf);
         names = new ArrayList<>(size);
      }
      while (names.size() < size) {
         names.add(ByteBufUtil.readString(buf));
         decoder.checkpoint();
      }
      complete(names);
   }
}
