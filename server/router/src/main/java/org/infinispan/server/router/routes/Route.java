package org.infinispan.server.router.routes;

public class Route<Source extends RouteSource, Destination extends RouteDestination> {

    private final Source routeSource;
    private final Destination routeDestination;

    public Route(Source routeSource, Destination routeDestination) {
        this.routeSource = routeSource;
        this.routeDestination = routeDestination;
    }

    public Destination getRouteDestination() {
        return routeDestination;
    }

    public Source getRouteSource() {
        return routeSource;
    }

    @Override
    public String toString() {
        return routeSource.toString() + "=>" + routeDestination.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Route route = (Route) o;

        if (!getRouteSource().equals(route.getRouteSource())) return false;
       return routeDestination.equals(route.routeDestination);
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
