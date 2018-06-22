package org.infinispan.rest.http2;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    private BlockingQueue<FullHttpResponse> responses = new LinkedBlockingQueue<>();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
        responses.add(msg);
        ctx.close();
    }

    public Queue<FullHttpResponse> getResponses() {
        return responses;
    }

    @Override
    public void sendRequest(FullHttpRequest request, SslContext sslContext, Channel channel) {
        HttpScheme scheme = sslContext != null ? HttpScheme.HTTPS : HttpScheme.HTTP;
        request.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), scheme.name());
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);
        HttpUtil.setContentLength(request, request.content().readableBytes());

        channel.writeAndFlush(request);
    }
}
