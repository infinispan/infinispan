package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Obtains a list of SASL authentication mechanisms supported by the server
 *
 * @since 14.0
 */
public class AuthMechListOperation extends HotRodOperation<List<String>> {
   private final Channel channel;
   private int mechCount = -1;
   private List<String> result;

   public AuthMechListOperation(OperationContext operationContext, Channel channel) {
      super(operationContext, AUTH_MECH_LIST_REQUEST, AUTH_MECH_LIST_RESPONSE, CacheOptions.DEFAULT);
      this.channel = channel;
   }

   @Override
   public CompletableFuture<List<String>> execute() {
      if (!channel.isActive()) {
         throw HOTROD.channelInactive(channel.remoteAddress(), channel.remoteAddress());
      }
      scheduleRead(channel);
      sendHeader(channel);
      return this;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (mechCount < 0) {
         mechCount = ByteBufUtil.readVInt(buf);
         result = new ArrayList<>(mechCount);
         decoder.checkpoint();
      }
      while (result.size() < mechCount) {
         result.add(ByteBufUtil.readString(buf));
         decoder.checkpoint();
      }
      complete(result);
   }
}
