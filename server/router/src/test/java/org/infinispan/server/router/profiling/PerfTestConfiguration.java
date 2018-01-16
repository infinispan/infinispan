package org.infinispan.server.router.profiling;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.Router;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.RouteSource;

public interface PerfTestConfiguration {

    List<HotRodServer> initServers();

    RemoteCacheManager initClient(Optional<Router> router, Optional<Set<Route<? extends RouteSource, ? extends RouteDestination>>> routes, List<HotRodServer> servers);

    default Optional<Set<Route<? extends RouteSource, ? extends RouteDestination>>> initRoutes(List<HotRodServer> servers) {
        return Optional.empty();
    }

    default Optional<Router> initRouter(Optional<Set<Route<? extends RouteSource, ? extends RouteDestination>>> routes) {
        return Optional.empty();
    }

    default void shutdown(List<HotRodServer> servers, Optional<Router> router) {
        servers.forEach(s -> s.stop());
        router.ifPresent(r -> r.stop());
    }

    default void performLoadTesting(RemoteCacheManager client, int numberOfIterations) {
        String keyPrefix = UUID.randomUUID().toString();
        RemoteCache<String, String> cache = client.getCache();
        IntStream.range(0, numberOfIterations).forEach(i -> cache.put(keyPrefix + i, "val" + i));
    }

}
