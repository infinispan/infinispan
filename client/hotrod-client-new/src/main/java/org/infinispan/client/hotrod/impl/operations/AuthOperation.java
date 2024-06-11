package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Performs a step in the challenge/response authentication operation
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthOperation extends AbstractNoCacheHotRodOperation<byte[]> {
   private final String saslMechanism;
   private final byte[] response;

   public AuthOperation(String saslMechanism, byte[] response) {
      this.saslMechanism = saslMechanism;
      this.response = response;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      byte[] saslMechBytes = saslMechanism.getBytes(HOTROD_STRING_CHARSET);
      ByteBufUtil.writeArray(buf, saslMechBytes);
      ByteBufUtil.writeArray(buf, response);
   }

   @Override
   public byte[] createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      boolean complete = buf.readUnsignedByte() > 0;
      byte[] challenge = ByteBufUtil.readArray(buf);
      return complete ? null : challenge;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.AUTH_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.AUTH_RESPONSE;
   }

   @Override
   public boolean forceSend() {
      return true;
   }
}
