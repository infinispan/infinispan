package org.infinispan.server.router.routes;

import org.infinispan.server.core.ProtocolServer;

public interface RouteDestination<T extends ProtocolServer> {
    default void validate() {
    }

    T getProtocolServer();
}
