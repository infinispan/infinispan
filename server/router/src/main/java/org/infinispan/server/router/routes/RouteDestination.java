package org.infinispan.server.router.routes;

import java.util.Objects;

import org.infinispan.server.core.ProtocolServer;

public abstract class RouteDestination<T extends ProtocolServer> {
    private final String name;
    private final T server;

    protected RouteDestination(String name, T server) {
        this.name = Objects.requireNonNull(name);
        this.server = Objects.requireNonNull(server);
    }

    public String getName() {
        return name;
    }

    public T getProtocolServer() {
        return server;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "name='" + name + "\'}";
    }
}
