package org.infinispan.server.router.routes.memcached;

import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.router.routes.RouteDestination;

public class MemcachedServerRouteDestination extends RouteDestination<MemcachedServer> {
   public MemcachedServerRouteDestination(String name, MemcachedServer memcachedServer) {
      super(name, memcachedServer);
   }
}
