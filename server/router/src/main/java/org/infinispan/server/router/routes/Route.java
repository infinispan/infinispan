package org.infinispan.server.router.routes;

public class Route<Source extends RouteSource, Destination extends RouteDestination> {

    private final Source routeSource;
    private final Destination routeDestination;

    public Route(Source routeSource, Destination routeDestination) {
        this.routeSource = routeSource;
        this.routeDestination = routeDestination;
    }

    public Destination getRouteDesitnation() {
        return routeDestination;
    }

    public Source getRouteSource() {
        return routeSource;
    }

    @Override
    public String toString() {
        return "Route{" +
                "routeSource=" + routeSource +
                ", routeDestination=" + routeDestination +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Route route = (Route) o;

        if (!getRouteSource().equals(route.getRouteSource())) return false;
        if (!routeDestination.equals(route.routeDestination)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getRouteSource().hashCode();
        result = 31 * result + routeDestination.hashCode();
        return result;
    }

    public void validate() {

    }
}
