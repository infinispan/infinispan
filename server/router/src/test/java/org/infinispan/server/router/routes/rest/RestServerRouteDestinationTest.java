package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.RestServer;
import org.junit.Test;

public class RestServerRouteDestinationTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateName() throws Exception {
        new RestServerRouteDestination(null, new RestServer()).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateRestResource() throws Exception {
        new RestServerRouteDestination("test", null).validate();
    }

}
