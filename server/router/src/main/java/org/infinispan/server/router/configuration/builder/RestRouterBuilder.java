package org.infinispan.server.router.configuration.builder;

import org.infinispan.server.router.configuration.RestRouterConfiguration;

/**
 * Configuration builder for REST.
 *
 * @author Sebastian Łaskawiec
 */
public class RestRouterBuilder extends AbstractRouterBuilder {

    /**
     * Creates new {@link RestRouterConfiguration}.
     *
     * @param parent Parent {@link ConfigurationBuilderParent}.
     */
    public RestRouterBuilder(ConfigurationBuilderParent parent) {
        super(parent);
    }

    /**
     * Builds {@link RestRouterConfiguration}.
     */
    public RestRouterConfiguration build() {
        if (this.enabled) {
            try {
                validate();
            } catch (Exception e) {
                throw logger.configurationValidationError(e);
            }
            return new RestRouterConfiguration(ip, port);
        }
        return null;
    }
}
