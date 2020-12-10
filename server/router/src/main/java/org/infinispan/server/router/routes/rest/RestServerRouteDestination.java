package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.RestServer;
import org.infinispan.server.router.routes.RouteDestination;

public class RestServerRouteDestination implements RouteDestination<RestServer> {

    private final String name;
    private final RestServer restServer;

    public RestServerRouteDestination(String name, RestServer restServer) {
        this.name = name;
        this.restServer = restServer;
    }

    public String getName() {
        return name;
    }

    @Override
    public RestServer getProtocolServer() {
        return restServer;
    }

    @Override
    public String toString() {
        return "RestServerRouteDestination{" +
                "name='" + name + '}';
    }

    @Override
    public void validate() {
        if (name == null || "".equals(name)) {
            throw new IllegalArgumentException("Name can not be null");
        }
        if (restServer == null) {
            throw new IllegalArgumentException("REST resource can not be null");
        }
    }

}
