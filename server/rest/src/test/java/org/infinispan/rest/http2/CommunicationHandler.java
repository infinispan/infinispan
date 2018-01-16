package org.infinispan.rest.http2;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.ssl.SslContext;

import java.util.Queue;

/**
 * Handles communication for both HTTP/2 and HTTP/1.1
 */
public interface CommunicationHandler {

    /**
     * @return Gets queued HTTP responses.
     */
    Queue<FullHttpResponse> getResponses();

    /**
     * Sends a message through the communication channel.
     *
     * @param request HTTP request.
     * @param sslContext SSL Context if applicable. Can be <code>null</code>.
     * @param channel Netty channel.
     */
    void sendRequest(FullHttpRequest request, SslContext sslContext, Channel channel);

}
