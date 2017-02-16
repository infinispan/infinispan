package org.infinispan.server.router.routes.rest;

import org.infinispan.server.router.routes.PrefixedRouteSource;

public class RestRouteSource implements PrefixedRouteSource {

    private final String pathPrefix;

    public RestRouteSource(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    @Override
    public String getRoutePrefix() {
        return pathPrefix;
    }

    @Override
    public String toString() {
        return "RestRouteSource{" +
                "pathPrefix='" + pathPrefix + '\'' +
                '}';
    }

    @Override
    public void validate() {
        if (pathPrefix == null || !pathPrefix.matches("\\w+")) {
            throw new IllegalArgumentException("Path is incorrect");
        }
    }
}
