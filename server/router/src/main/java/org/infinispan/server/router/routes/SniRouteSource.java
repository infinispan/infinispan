package org.infinispan.server.router.routes;

import javax.net.ssl.SSLContext;

public interface SniRouteSource extends RouteSource {

    SSLContext getSslContext();

    String getSniHostName();

}
