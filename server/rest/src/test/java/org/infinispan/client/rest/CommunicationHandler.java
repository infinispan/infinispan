package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.ssl.SslContext;

/**
 * Handles communication for both HTTP/2 and HTTP/1.1
 */
public interface CommunicationHandler {
    /**
     * Sends a message through the communication channel.
     *  @param request HTTP request.
     * @param sslContext SSL Context if applicable. Can be <code>null</code>.
     * @param channel Netty channel.
     * @return
     */
    CompletionStage<FullHttpResponse> sendRequest(FullHttpRequest request, SslContext sslContext, Channel channel);

}
