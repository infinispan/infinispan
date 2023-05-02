package org.infinispan.client.hotrod.counter.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
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

   public GetCounterNamesOperation(ChannelFactory transportFactory, AtomicReference<ClientTopology> topologyId,
                                   Configuration cfg) {
      super(COUNTER_GET_NAMES_REQUEST, COUNTER_GET_NAMES_RESPONSE, transportFactory, topologyId, cfg, "", false);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendHeader(channel);
      setCacheName();
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      codec.writeHeader(buf, header);
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
         decoder.checkpoint();
      }
      while (names.size() < size) {
         names.add(ByteBufUtil.readString(buf));
         decoder.checkpoint();
      }
      complete(names);
   }
}
