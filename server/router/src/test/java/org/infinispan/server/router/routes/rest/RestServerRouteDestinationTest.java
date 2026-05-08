package org.infinispan.server.router.routes.rest;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.infinispan.rest.RestServer;
import org.junit.jupiter.api.Test;

public class RestServerRouteDestinationTest {

    @Test
    public void shouldValidateName() {
       assertThrows(NullPointerException.class, () -> new RestServerRouteDestination(null, new RestServer()));
    }

    @Test
    public void shouldValidateRestResource() {
       assertThrows(NullPointerException.class, () -> new RestServerRouteDestination("test", null));
    }

}
