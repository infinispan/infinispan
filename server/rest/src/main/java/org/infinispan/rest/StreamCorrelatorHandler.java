package org.infinispan.rest;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.AsciiString;

/**
 * Handler to propagate HTTP/2 StreamId between requests and responses.
 *
 * @since 12.0
 */
class StreamCorrelatorHandler extends ChannelDuplexHandler {

   public static final AsciiString STREAM_ID_HEADER = HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text();
   private Integer streamId;

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (msg instanceof HttpRequest) {
         HttpRequest request = (HttpRequest) msg;
         streamId = request.headers().getInt(STREAM_ID_HEADER);
      }
      ctx.fireChannelRead(msg);
   }

   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
      if (msg instanceof HttpResponse) {
         HttpResponse response = (HttpResponse) msg;
         if (streamId != null) {
            response.headers().add(STREAM_ID_HEADER, streamId);
         }
      }
      ctx.write(msg, promise);
   }
}
