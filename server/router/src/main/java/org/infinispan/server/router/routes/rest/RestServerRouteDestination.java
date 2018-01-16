package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.RestServer;
import org.infinispan.server.router.routes.RouteDestination;

public class RestServerRouteDestination implements RouteDestination {

    private final String name;
    private final RestServer restServer;

    public RestServerRouteDestination(String name, RestServer restServer) {
        this.name = name;
        this.restServer = restServer;
    }

    public String getName() {
        return name;
    }

    public RestServer getRestServer() {
        return restServer;
    }

    @Override
    public String toString() {
        return "RestServerRouteDestination{" +
                "name='" + name + '\'' +
                ", restServer=" + restServer +
                '}';
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
