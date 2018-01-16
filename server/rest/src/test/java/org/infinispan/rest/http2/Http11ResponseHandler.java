package org.infinispan.rest.http2;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslContext;

/**
 * Process {@link FullHttpResponse} for HTTP/1.1.
 */
public class Http11ResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> implements CommunicationHandler {

    private final Queue<FullHttpResponse> responses = new LinkedBlockingQueue<>();

    private final Semaphore semaphore = new Semaphore(1);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        responses.add(msg);
        semaphore.release();
        System.out.println("Released");
    }

    public Queue<FullHttpResponse> getResponses() {
        try {
            System.out.println("Getting responses");
            semaphore.acquireUninterruptibly();
            return responses;
        } finally {
            semaphore.release();
        }
    }

    @Override
    public void sendRequest(FullHttpRequest request, SslContext sslContext, Channel channel) {
        HttpScheme scheme = sslContext != null ? HttpScheme.HTTPS : HttpScheme.HTTP;
        request.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), scheme.name());
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);

        channel.writeAndFlush(request);
        semaphore.acquireUninterruptibly();
        System.out.println("Acquired");
    }
}
