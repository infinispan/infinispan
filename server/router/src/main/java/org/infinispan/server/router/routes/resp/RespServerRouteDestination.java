package org.infinispan.server.router.routes.resp;

import org.infinispan.server.resp.RespServer;
import org.infinispan.server.router.routes.RouteDestination;

public class RespServerRouteDestination implements RouteDestination<RespServer> {

   private final String name;
   private final RespServer respServer;

   public RespServerRouteDestination(String name, RespServer respServer) {
      this.name = name;
      this.respServer = respServer;
   }

   @Override
   public RespServer getProtocolServer() {
      return respServer;
   }

   @Override
   public String toString() {
      return "RespServerRouteDestination{" +
            "name='" + name + '}';
   }

   @Override
   public void validate() {
      if (name == null || "".equals(name)) {
         throw new IllegalArgumentException("Name can not be null");
      }
      if (respServer == null) {
         throw new IllegalArgumentException("Server can not be null");
      }
   }
}
