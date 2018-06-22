package org.infinispan.client.hotrod.impl.transport.netty.singleport;

import org.infinispan.client.hotrod.impl.transport.netty.ActivationHandler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;

/**
 * Handles upgrades for multiple protocols.
 *
 * @author Sebastian Łaskawiec
 */
public class SinglePortAdapter extends ChannelInitializer<Channel> {

   public static final int HTTP_MAX_CONTENT_LENGTH = 1024 * 10;

   private final CustomProtocolUpgradeCodec targetProtocol;
   private final SslHandlerFactory sslHandlerFactory;

   public SinglePortAdapter(SslHandlerFactory sslHandlerFactory, CustomProtocolUpgradeCodec targetProtocol) {
      this.sslHandlerFactory = sslHandlerFactory;
      this.targetProtocol = targetProtocol;
   }

   @Override
   protected void initChannel(Channel ch) {
      if (sslHandlerFactory != null) {
         configureSsl(ch);
      } else {
         configureClearText(ch);
      }
   }

   private void configureClearText(Channel ch) {
      HttpClientCodec sourceCodec = new HttpClientCodec();
      CustomProtocolUpgradeCodec upgradeCodec = targetProtocol;
      HttpClientUpgradeHandler upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, HTTP_MAX_CONTENT_LENGTH);

      ch.pipeline().addLast(sourceCodec, upgradeHandler, new UpgradeRequestSenderHandler(), new ChannelInboundHandlerAdapter() {

         @Override
         public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt == HttpClientUpgradeHandler.UpgradeEvent.UPGRADE_SUCCESSFUL) {
               // All handlers in the upgrade procedure are responsible for removing themselves from
               // the pipeline once we finish.
               ctx.pipeline().remove(this);
               fireActivateEvent(ctx.channel());
            }
            ctx.fireUserEventTriggered(evt);
         }
      });
   }

   private void configureSsl(Channel ch) {
      sslHandlerFactory.assertAlpnSupported();
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast(null, sslHandlerFactory.getSslHandlerName(), sslHandlerFactory.getSslHandler(ch.alloc()));
      //The ALPN needs to be added after SSL.
      pipeline.addAfter(sslHandlerFactory.getSslHandlerName(), "alpn", new ApplicationProtocolNegotiationHandler("Unknown") {
         @Override
         protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
            if (!targetProtocol.protocol().equals(protocol)) {
               ctx.close();
               throw new IllegalStateException("Unknown protocol: " + protocol);
            }
            ctx.pipeline().addLast(targetProtocol.getHandler());
            fireActivateEvent(ch);
         }
      });
   }

   private void fireActivateEvent(Channel channel) {
      channel.pipeline().fireUserEventTriggered(ActivationHandler.ACTIVATION_EVENT);
   }

}
