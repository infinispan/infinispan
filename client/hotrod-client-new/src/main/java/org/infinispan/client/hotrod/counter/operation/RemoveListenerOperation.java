package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.Handle;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A remove listener operation for {@link Handle#remove()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;

   public RemoveListenerOperation(String counterName, byte[] listenerId) {
      super(counterName, false);
      this.listenerId = listenerId;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      ByteBufUtil.writeArray(buf, listenerId);
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      if (status == NO_ERROR_STATUS) {
         decoder.removeListener(listenerId);
      }
      return status == NO_ERROR_STATUS;
   }

   @Override
   public short requestOpCode() {
      return COUNTER_REMOVE_LISTENER_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_REMOVE_LISTENER_RESPONSE;
   }
}
