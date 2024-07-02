package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;
import net.jcip.annotations.Immutable;

/**
 * Corresponds to the "ping" operation as defined in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PingOperation extends NeutralVersionHotRodOperation<PingResponse> implements ChannelOperation {
   private static final Log log = LogFactory.getLog(PingOperation.class);

   private final boolean releaseChannel;

   private final PingResponse.Decoder responseBuilder;

   public PingOperation(Codec codec, AtomicReference<ClientTopology> clientTopology, Configuration cfg, byte[] cacheName, ChannelFactory channelFactory, boolean releaseChannel) {
      this(PING_REQUEST, PING_RESPONSE, codec, clientTopology, cfg, cacheName, channelFactory, releaseChannel);
   }

   protected PingOperation(short requestCode, short responseCode, Codec codec, AtomicReference<ClientTopology> clientTopology, Configuration cfg, byte[] cacheName,
                           ChannelFactory channelFactory, boolean releaseChannel) {
      super(requestCode, responseCode, codec, 0, cfg, cacheName, clientTopology, channelFactory);
      this.releaseChannel = releaseChannel;
      this.responseBuilder = new PingResponse.Decoder(cfg.version());
   }

   @Override
   public void invoke(Channel channel) {
      sendHeaderAndRead(channel);
      if (releaseChannel) {
         releaseChannel(channel);
      }
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buffer) {
      codec.writeHeader(buffer, header);
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      completeExceptionally(cause);
   }

   @Override
   public CompletableFuture<PingResponse> execute() {
      throw new UnsupportedOperationException("Cannot execute directly");
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      responseBuilder.processResponse(codec, buf, decoder);
      if (HotRodConstants.isSuccess(status)) {
         PingResponse pingResponse = responseBuilder.build(status);
         if (pingResponse.getVersion() != null && cfg.version() == ProtocolVersion.PROTOCOL_VERSION_AUTO) {
            channelFactory.setNegotiatedCodec(pingResponse.getVersion().getCodec());
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
   public void exceptionCaught(Channel channel, Throwable cause) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      PingResponse pingResponse = new PingResponse(cause);
      if (pingResponse.isCacheNotFound()) {
         complete(pingResponse);
      } else {
         super.exceptionCaught(channel, cause);
      }
   }
}
