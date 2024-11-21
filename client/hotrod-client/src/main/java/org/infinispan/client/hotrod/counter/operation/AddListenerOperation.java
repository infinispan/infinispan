package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * An add listener operation for {@link StrongCounter#addListener(CounterListener)} and {@link
 * WeakCounter#addListener(CounterListener)}
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class AddListenerOperation extends BaseCounterOperation<Channel> {

   private final byte[] listenerId;

   public AddListenerOperation(String counterName, byte[] listenerId) {
      super(counterName, false);
      this.listenerId = listenerId;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      ByteBufUtil.writeArray(buf, listenerId);
   }

   @Override
   public Channel createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      if (status != NO_ERROR_STATUS) {
         return null;
      } else {
         return decoder.getChannel();
      }
   }

   @Override
   public short requestOpCode() {
      return COUNTER_ADD_LISTENER_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_ADD_LISTENER_RESPONSE;
   }
}
