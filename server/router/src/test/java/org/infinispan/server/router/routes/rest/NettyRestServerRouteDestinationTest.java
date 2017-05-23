package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.RestServer;
import org.junit.Test;

public class NettyRestServerRouteDestinationTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateName() throws Exception {
        new NettyRestServerRouteDestination(null, new RestServer()).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateRestResource() throws Exception {
        new NettyRestServerRouteDestination("test", null).validate();
    }

}
