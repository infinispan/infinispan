package org.infinispan.client.rest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.PlatformDependent;

/**
 * Process {@link io.netty.handler.codec.http.FullHttpResponse} translated from HTTP/2 frames
 */
public class Http2ResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> implements  CommunicationHandler {
    private final Map<Integer, CompletableFuture<FullHttpResponse>> responses;

    //Netty uses stream ids to separate concurrent conversations. It seems to be an implementation details but this counter
    //get always incremented by 2
    private final AtomicInteger streamCounter = new AtomicInteger(3);

    public Http2ResponseHandler() {
        // Use a concurrent map because we add and iterate from the main thread (just for the purposes of the example),
        // but Netty also does a get on the map when messages are received in a EventLoop thread.
        responses = PlatformDependent.newConcurrentHashMap();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
        Integer streamId = msg.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
        if (streamId == null) {
            throw new IllegalArgumentException("Http2ResponseHandler unexpected message received: " + msg);
        }

        CompletableFuture<FullHttpResponse> entry = responses.remove(streamId);
        if (entry == null) {
            throw new IllegalArgumentException("Message received for unknown stream id " + streamId);
        } else {
            entry.complete(msg);
        }
    }

    @Override
    public CompletableFuture<FullHttpResponse> sendRequest(FullHttpRequest request, SslContext sslContext, Channel channel) {
        int streamId = streamCounter.getAndAdd(2);
        HttpScheme scheme = sslContext != null ? HttpScheme.HTTPS : HttpScheme.HTTP;

        request.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), scheme.name());
        request.headers().add(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);

        channel.write(request);
        channel.flush();
        CompletableFuture<FullHttpResponse> response = new CompletableFuture<>();
        responses.put(streamId, response);
        return response;
    }
}
