package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.Server;
import org.junit.Test;

public class NettyRestServerRouteDestinationTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateName() throws Exception {
        new NettyRestServerRouteDestination(null, new Server(null, null)).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateRestResource() throws Exception {
        new NettyRestServerRouteDestination("test", null).validate();
    }

}
