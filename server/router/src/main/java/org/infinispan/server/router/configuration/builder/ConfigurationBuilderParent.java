package org.infinispan.server.router.configuration.builder;

/**
 * Router configuration builder.
 *
 * @author Sebastian Łaskawiec
 */
public interface ConfigurationBuilderParent {

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

    /**
     * Returns builder for Single Port.
     */
    SinglePortRouterBuilder singlePort();
}
