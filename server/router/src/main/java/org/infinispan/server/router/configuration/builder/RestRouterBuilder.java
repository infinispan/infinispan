package org.infinispan.server.router.configuration.builder;

import org.infinispan.server.router.configuration.RestRouterConfiguration;
import org.infinispan.server.router.logging.RouterLogger;

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
                throw RouterLogger.SERVER.configurationValidationError(e);
            }
            return new RestRouterConfiguration(ip, port);
        }
        return null;
    }
}
