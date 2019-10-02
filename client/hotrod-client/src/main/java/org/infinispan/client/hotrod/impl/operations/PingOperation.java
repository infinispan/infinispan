package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
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
public class PingOperation extends HotRodOperation<PingResponse> implements ChannelOperation {
   private static final Log log = LogFactory.getLog(PingOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   private final boolean releaseChannel;
   private final OperationsFactory operationsFactory;

   private final PingResponse.Decoder responseBuilder;

   public PingOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, byte[] cacheName, ChannelFactory channelFactory, boolean releaseChannel, OperationsFactory operationsFactory) {
      this(PING_REQUEST, PING_RESPONSE, codec, topologyId, cfg, cacheName, channelFactory, releaseChannel, operationsFactory);
   }

   protected PingOperation(short requestCode, short responseCode, Codec codec, AtomicInteger topologyId, Configuration cfg, byte[] cacheName,
                           ChannelFactory channelFactory, boolean releaseChannel, OperationsFactory operationsFactory) {
      super(requestCode, responseCode, codec, 0, cfg, cacheName, topologyId, channelFactory);
      this.releaseChannel = releaseChannel;
      this.operationsFactory = operationsFactory;
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
