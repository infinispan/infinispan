package org.infinispan.rest.http2;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
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

    private final TimeUnit TIMEOUT_UNITS = TimeUnit.MINUTES;
    private final int TIMEOUT = 15;

    private final Map<Integer, Entry<ChannelFuture, ChannelPromise>> streamidPromiseMap;
    private final Queue<FullHttpResponse> responses = new LinkedBlockingQueue<>();

    //Netty uses stream ids to separate concurrent conversations. It seems to be an implementation details but this counter
    //get always incremented by 2
    private final AtomicInteger streamCounter = new AtomicInteger(3);

    public Http2ResponseHandler() {
        // Use a concurrent map because we add and iterate from the main thread (just for the purposes of the example),
        // but Netty also does a get on the map when messages are received in a EventLoop thread.
        streamidPromiseMap = PlatformDependent.newConcurrentHashMap();
    }

    /**
     * Create an association between an anticipated response stream id and a {@link io.netty.channel.ChannelPromise}
     *
     * @param streamId The stream for which a response is expected
     * @param writeFuture A future that represent the request write operation
     * @param promise The promise object that will be used to wait/notify events
     * @return The previous object associated with {@code streamId}
     * @see Http2ResponseHandler#awaitResponses(long, java.util.concurrent.TimeUnit)
     */
    public Entry<ChannelFuture, ChannelPromise> put(int streamId, ChannelFuture writeFuture, ChannelPromise promise) {
        return streamidPromiseMap.put(streamId, new SimpleEntry<ChannelFuture, ChannelPromise>(writeFuture, promise));
    }

    private void awaitResponses(long timeout, TimeUnit unit) {
        Iterator<Entry<Integer, Entry<ChannelFuture, ChannelPromise>>> itr = streamidPromiseMap.entrySet().iterator();
        while (itr.hasNext()) {
            Entry<Integer, Entry<ChannelFuture, ChannelPromise>> entry = itr.next();
            ChannelFuture writeFuture = entry.getValue().getKey();
            if (!writeFuture.awaitUninterruptibly(timeout, unit)) {
                throw new IllegalStateException("Timed out waiting to write for stream id " + entry.getKey());
            }
            if (!writeFuture.isSuccess()) {
                throw new RuntimeException(writeFuture.cause());
            }
            ChannelPromise promise = entry.getValue().getValue();
            if (!promise.awaitUninterruptibly(timeout, unit)) {
                throw new IllegalStateException("Timed out waiting for response on stream id " + entry.getKey());
            }
            if (!promise.isSuccess()) {
                throw new RuntimeException(promise.cause());
            }
            itr.remove();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        Integer streamId = msg.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
        if (streamId == null) {
            throw new IllegalArgumentException("Http2ResponseHandler unexpected message received: " + msg);
        }

        Entry<ChannelFuture, ChannelPromise> entry = streamidPromiseMap.get(streamId);
        if (entry == null) {
            throw new IllegalArgumentException("Message received for unknown stream id " + streamId);
        } else {
            responses.add(msg);
            entry.getValue().setSuccess();
        }
    }

    @Override
    public Queue<FullHttpResponse> getResponses() {
        awaitResponses(TIMEOUT, TIMEOUT_UNITS);
        return responses;
    }

    @Override
    public void sendRequest(FullHttpRequest request, SslContext sslContext, Channel channel) {
        int streamId = streamCounter.getAndAdd(2);
        HttpScheme scheme = sslContext != null ? HttpScheme.HTTPS : HttpScheme.HTTP;

        request.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), scheme.name());
        request.headers().add(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
        put(streamId, channel.write(request), channel.newPromise());
        channel.flush();
    }
}
