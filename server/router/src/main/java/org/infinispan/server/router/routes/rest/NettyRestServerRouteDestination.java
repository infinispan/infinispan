package org.infinispan.server.router.routes.rest;

import org.infinispan.rest.Server;
import org.infinispan.server.router.routes.RouteDestination;

public class NettyRestServerRouteDestination implements RouteDestination {

    private final String name;
    private final Server restResource;

    public NettyRestServerRouteDestination(String name, Server restResource) {
        this.name = name;
        this.restResource = restResource;
    }

    public String getName() {
        return name;
    }

    public Server getRestResource() {
        return restResource;
    }

    @Override
    public String toString() {
        return "NettyRestServerRouteDestination{" +
                "name='" + name + '\'' +
                ", restResource=" + restResource +
                '}';
    }

    @Override
    public void validate() {
        if (name == null || "".equals(name)) {
            throw new IllegalArgumentException("Name can not be null");
        }
        if (restResource == null) {
            throw new IllegalArgumentException("REST resource can not be null");
        }
    }
}
