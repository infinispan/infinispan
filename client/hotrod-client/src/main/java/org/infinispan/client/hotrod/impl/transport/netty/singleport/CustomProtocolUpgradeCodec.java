package org.infinispan.client.hotrod.impl.transport.netty.singleport;

import java.util.Collection;
import java.util.Collections;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpRequest;

/**
 * Codec responsible for switching to target protocol.
 *
 * @author Sebastian Łaskawiec
 */
public class CustomProtocolUpgradeCodec implements HttpClientUpgradeHandler.UpgradeCodec {

   private final String protocolName;
   private final ChannelHandler handler;

   public CustomProtocolUpgradeCodec(String protocolName, ChannelHandler handler) {
      this.protocolName = protocolName;
      this.handler = handler;
   }

   @Override
   public String protocol() {
      return protocolName;
   }

   @Override
   public Collection<CharSequence> setUpgradeHeaders(ChannelHandlerContext ctx, HttpRequest upgradeRequest) {
      return Collections.emptyList();
   }

   @Override
   public void upgradeTo(ChannelHandlerContext ctx, FullHttpResponse upgradeResponse) throws Exception {
      // Since we reached this state, we know that the whole upgrade procedure (client sending HTTP/1.1 Upgrade,
      // and Server responding with Switching Protocols) went fine. No need for validation, just update
      // the pipeline and we are done.
      ctx.pipeline().addAfter(ctx.name(), null, handler);
   }

   public ChannelHandler getHandler() {
      return handler;
   }
}
