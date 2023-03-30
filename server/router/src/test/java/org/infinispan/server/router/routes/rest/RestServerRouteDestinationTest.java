package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.RestServer;
import org.junit.Test;

public class RestServerRouteDestinationTest {

    @Test(expected = NullPointerException.class)
    public void shouldValidateName() {
        new RestServerRouteDestination(null, new RestServer());
    }

    @Test(expected = NullPointerException.class)
    public void shouldValidateRestResource() {
        new RestServerRouteDestination("test", null);
    }

}
