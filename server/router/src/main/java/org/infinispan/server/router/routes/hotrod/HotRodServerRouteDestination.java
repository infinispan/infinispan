package org.infinispan.server.router.routes.hotrod;

import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.routes.RouteDestination;

public class HotRodServerRouteDestination extends RouteDestination<HotRodServer> {

   public HotRodServerRouteDestination(String name, HotRodServer hotRodServer) {
      super(name, hotRodServer);
   }
}
