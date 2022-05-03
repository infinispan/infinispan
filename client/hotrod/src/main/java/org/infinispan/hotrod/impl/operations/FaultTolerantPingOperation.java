package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.configuration.ProtocolVersion;
import org.infinispan.hotrod.exceptions.InvalidResponseException;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;

/**
 * A fault tolerant ping operation that can survive to node failures.
 *
 * @since 14.0
 */
public class FaultTolerantPingOperation extends RetryOnFailureOperation<PingResponse> {
   private final PingResponse.Decoder responseBuilder;

   protected FaultTolerantPingOperation(OperationContext operationContext, CacheOptions options) {
      super(operationContext, PING_REQUEST, PING_RESPONSE, options, null);
      this.responseBuilder = new PingResponse.Decoder(operationContext.getConfiguration().version());
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      responseBuilder.processResponse(operationContext.getCodec(), buf, decoder);
      if (HotRodConstants.isSuccess(status)) {
         PingResponse pingResponse = responseBuilder.build(status);
         if (pingResponse.getVersion() != null && operationContext.getConfiguration().version() == ProtocolVersion.PROTOCOL_VERSION_AUTO) {
            operationContext.setCodec(Codec.forProtocol(pingResponse.getVersion()));
         }
         complete(pingResponse);
      } else {
         String hexStatus = Integer.toHexString(status);
         if (log.isTraceEnabled())
            log.tracef("Unknown response status: %s", hexStatus);

         throw new InvalidResponseException("Unexpected response status: " + hexStatus);
      }
   }

   @Override
   protected Throwable handleException(Throwable cause, Channel channel, SocketAddress address) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      PingResponse pingResponse = new PingResponse(cause);
      if (pingResponse.isCacheNotFound()) {
         complete(pingResponse);
         return null;
      }
      return super.handleException(cause, channel, address);
   }

   @Override
   protected void reset() {
      super.reset();
      responseBuilder.reset();
   }
}
