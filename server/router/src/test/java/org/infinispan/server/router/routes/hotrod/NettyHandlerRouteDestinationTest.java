package org.infinispan.server.router.routes.hotrod;

import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public class NettyHandlerRouteDestinationTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateName() throws Exception {
        new NettyHandlerRouteDestination(null, new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                //does not matter
            }
        }).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateChannelInitializer() throws Exception {
        new NettyHandlerRouteDestination("test", null).validate();
    }

}
