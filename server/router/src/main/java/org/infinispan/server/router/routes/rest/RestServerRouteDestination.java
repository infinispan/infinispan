package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.RestServer;
import org.infinispan.server.router.routes.RouteDestination;

public class RestServerRouteDestination extends RouteDestination<RestServer> {

    public RestServerRouteDestination(String name, RestServer restServer) {
        super(name, restServer);
    }
}
