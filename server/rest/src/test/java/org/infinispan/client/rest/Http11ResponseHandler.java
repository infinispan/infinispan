package org.infinispan.client.rest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslContext;

/**
 * Process {@link FullHttpResponse} for HTTP/1.1.
 */
public class Http11ResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> implements CommunicationHandler {
    private CompletableFuture<FullHttpResponse> response;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
        response.complete(msg);
        ctx.close();
    }

    @Override
    public CompletionStage<FullHttpResponse> sendRequest(FullHttpRequest request, SslContext sslContext, Channel channel) {
        HttpScheme scheme = sslContext != null ? HttpScheme.HTTPS : HttpScheme.HTTP;
        request.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), scheme.name());
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);
        HttpUtil.setContentLength(request, request.content().readableBytes());

        channel.writeAndFlush(request);

        response = new CompletableFuture<>();
        return response;
    }
}
