package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;

/**
 * A fault tolerant ping operation that can survive to node failures.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class FaultTolerantPingOperation extends RetryOnFailureOperation<PingResponse> {

   private final OperationsFactory operationsFactory;

   private PingResponse.Decoder responseBuilder;

   protected FaultTolerantPingOperation(Codec codec, ChannelFactory channelFactory,
                                        byte[] cacheName, AtomicInteger topologyId, int flags,
                                        Configuration cfg, OperationsFactory operationsFactory) {
      super(PING_REQUEST, PING_RESPONSE, codec, channelFactory, cacheName, topologyId, flags, cfg, null);
      this.operationsFactory = operationsFactory;
      this.responseBuilder = new PingResponse.Decoder(cfg.version());
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      responseBuilder.processResponse(codec, buf, decoder);
      if (HotRodConstants.isSuccess(status)) {
         PingResponse pingResponse = responseBuilder.build(status);
         if (pingResponse.getVersion() != null && cfg.version() == ProtocolVersion.PROTOCOL_VERSION_AUTO) {
            operationsFactory.setCodec(pingResponse.getVersion().getCodec());
         }
         complete(pingResponse);
      } else {
         String hexStatus = Integer.toHexString(status);
         if (trace)
            log.tracef("Unknown response status: %s", hexStatus);

         throw new InvalidResponseException("Unexpected response status: " + hexStatus);
      }
   }

   @Override
   protected Throwable handleException(Throwable cause, ChannelHandlerContext ctx, SocketAddress address) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      PingResponse pingResponse = new PingResponse(cause);
      if (pingResponse.isCacheNotFound()) {
         complete(pingResponse);
         return null;
      }
      return super.handleException(cause, ctx, address);
   }

   @Override
   protected void reset() {
      super.reset();
      responseBuilder.reset();
   }
}
