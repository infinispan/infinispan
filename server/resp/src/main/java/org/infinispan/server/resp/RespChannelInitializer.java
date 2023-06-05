package org.infinispan.server.resp;

import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.core.transport.NettyInitializer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

/**
 * Creates Netty Channels for the resp server.
 *
 * @author William Burns
 */
public class RespChannelInitializer implements NettyInitializer {

   private final RespServer respServer;

   /**
    * Creates new {@link RespChannelInitializer}.
    *
    * @param respServer Resp Server this initializer belongs to.
    */
   public RespChannelInitializer(RespServer respServer) {
      this.respServer = respServer;
   }

   @Override
   public void initializeChannel(Channel ch) {
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ch);
      metadata.protocolVersion("RESP3");
      ChannelPipeline pipeline = ch.pipeline();
      RespRequestHandler initialHandler;
      if (respServer.getConfiguration().authentication().enabled()) {
         initialHandler = new Resp3AuthHandler(respServer);
      } else {
         initialHandler = respServer.newHandler();
      }

      RespDecoder decoder = new RespDecoder();
      pipeline.addLast(decoder);
      pipeline.addLast(new RespHandler(decoder, initialHandler));
   }
}
