package org.infinispan.server.router;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.RouteSource;

/**
 * A container for routing information.
 *
 * @author Sebastian Łaskawiec
 */
public class RoutingTable {

    private Set<Route<? extends RouteSource, ? extends RouteDestination>> routes = new HashSet<>();

    /**
     * Creates new {@link RoutingTable}.
     *
     * @param routes A set of {@link Route}s for the routing table.
     */
    public RoutingTable(Set<Route<? extends RouteSource, ? extends RouteDestination>> routes) {
        this.routes.addAll(routes);
    }

    /**
     * Returns the number of {@link Route}s present in the routing table.
     */
    public int routesCount() {
        return routes.size();
    }

    /**
     * Returns a stream of all {@link Route}s in the routing table.
     */
    public Stream<Route<? extends RouteSource, ? extends RouteDestination>> streamRoutes() {
        return routes.stream();
    }

    /**
     * Returns a {@link Stream} of {@link Route}s matching the initial criteria
     *
     * @param sourceType      Class of the <code>Source</code> type.
     * @param destinationType Class of the <code>Desitnation</code> type.
     * @param <Source>        Type of the {@link RouteSource}
     * @param <Destination>   Type of the {@link RouteDestination}
     * @return
     */
    @SuppressWarnings("unchecked")
    public <Source extends RouteSource, Destination extends RouteDestination>
    Stream<Route<Source, Destination>> streamRoutes(Class<Source> sourceType, Class<Destination> destinationType) {
        //Unfortunately there is no nice way to do a cast here, so we need to un-generify this.
        Stream unGenerifiedStream = routes.stream()
                .filter(r -> sourceType.isAssignableFrom(r.getRouteSource().getClass()))
                .filter(r -> destinationType.isAssignableFrom(r.getRouteDesitnation().getClass()));

        return unGenerifiedStream;
    }

    @Override
    public String toString() {
        return "RoutingTable{" +
                "routes=" + routes +
                '}';
    }
}
