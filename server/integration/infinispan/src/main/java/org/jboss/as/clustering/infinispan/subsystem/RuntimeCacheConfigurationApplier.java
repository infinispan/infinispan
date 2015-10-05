package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.configuration.cache.Configuration;
import org.jboss.dmr.ModelNode;

/**
 * RuntimeCacheConfigurationApplier.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public interface RuntimeCacheConfigurationApplier {
    void applyConfiguration(Configuration configuration, ModelNode value);
}
