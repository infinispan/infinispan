package org.infinispan.server.router.routes;

public interface PrefixedRouteSource extends RouteSource {
    String getRoutePrefix();
}
