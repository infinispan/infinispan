package org.infinispan.server.router.routes.hotrod;

import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.routes.RouteDestination;

public class HotRodServerRouteDestination implements RouteDestination {

    private final String name;
    private final HotRodServer hotrodServer;

    public HotRodServerRouteDestination(String name, HotRodServer hotRodServer) {
        this.name = name;
        this.hotrodServer = hotRodServer;
    }

    public HotRodServer getHotrodServer() {
        return hotrodServer;
    }

    @Override
    public String toString() {
        return "HotRodServerRouteDestination{" +
              "name='" + name + '\'' +
              ", hotrodServer=" + hotrodServer +
              '}';
    }

    @Override
    public void validate() {
        if (name == null || "".equals(name)) {
            throw new IllegalArgumentException("Name can not be null");
        }
        if (hotrodServer == null) {
            throw new IllegalArgumentException("Channels can not be null");
        }
    }
}
