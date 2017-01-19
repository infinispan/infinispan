package org.infinispan.server.router.routes.hotrod;

import org.infinispan.server.router.routes.RouteDestination;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public class NettyHandlerRouteDestination implements RouteDestination {

    private final String name;
    private final ChannelInitializer<Channel> channels;

    public NettyHandlerRouteDestination(String name, ChannelInitializer<Channel> channels) {
        this.name = name;
        this.channels = channels;
    }

    public ChannelInitializer<Channel> getChannelInitializer() {
        return channels;
    }

    @Override
    public String toString() {
        return "NettyHandlerRouteDestination{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public void validate() {
        if (name == null || "".equals(name)) {
            throw new IllegalArgumentException("Name can not be null");
        }
        if (channels == null) {
            throw new IllegalArgumentException("Channels can not be null");
        }
    }
}
