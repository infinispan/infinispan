package org.infinispan.rest.http2;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * Initializer that supports post-initialize actions like initiating HTTP/2 upgrade.
 */
public abstract class CommunicationInitializer extends ChannelInitializer<SocketChannel> {

    /**
     * Initiates HTTP/2 upgrade if needed.
     *
     * @throws Exception Thrown on errors.
     */
    public void upgradeToHttp2IfNeeded() throws Exception {

    }

    /**
     * @return Gets Communication Handler instance.
     */
    public abstract CommunicationHandler getCommunicationHandler();

}
