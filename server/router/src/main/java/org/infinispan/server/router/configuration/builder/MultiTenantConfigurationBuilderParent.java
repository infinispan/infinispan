package org.infinispan.server.router.configuration.builder;

/**
 * Multi tenant router configuration builder.
 *
 * @author Sebastian Łaskawiec
 */
public interface MultiTenantConfigurationBuilderParent {

    /**
     * Returns builder for Routing Table.
     */
    RoutingBuilder routing();

    /**
     * Returns builder for Hot Rod.
     */
    HotRodRouterBuilder hotrod();

    /**
     * Returns builder for REST.
     */
    RestRouterBuilder rest();
}
