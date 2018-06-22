package org.infinispan.client.hotrod.impl.transport.netty.singleport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

/**
 * Sends an HTTP/1.1 Upgrade request on root path. This procedure initiates HTTP/1.1 Upgrade procedure.
 * The next step happens on the server side, where the server sends Switching Protocols header.
 *
 * @author Sebastian Łaskawiec
 */
class UpgradeRequestSenderHandler extends ChannelInboundHandlerAdapter {
   @Override
   public void channelActive(ChannelHandlerContext ctx) {
      DefaultFullHttpRequest upgradeRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

      SocketAddress address = ctx.channel().remoteAddress();
      String host = address.toString();
      if (address instanceof InetSocketAddress) {
         host = ((InetSocketAddress) address).getHostName();
      }

      // OpenShift uses Host header to figure out the destination Pod.
      upgradeRequest.headers().add("Host", host);
      ctx.writeAndFlush(upgradeRequest);
      ctx.pipeline().remove(this);
   }


}
