package org.infinispan.server.router.routes.resp;

import org.infinispan.server.resp.RespServer;
import org.infinispan.server.router.routes.RouteDestination;

public class RespServerRouteDestination extends RouteDestination<RespServer> {

   public RespServerRouteDestination(String name, RespServer respServer) {
      super(name, respServer);
   }
}
