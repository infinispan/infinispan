package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.StatePersistenceConfiguration;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.msc.value.InjectedValue;
import org.jboss.msc.value.Value;

/**
 * StatePersistenceConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class StatePersistenceConfigurationBuilder implements Value<StatePersistenceConfiguration>, StatePersistenceConfiguration {
    private final InjectedValue<PathManager> pathManager = new InjectedValue<>();
    private String path;
    private String relativeTo;

    @Override
    public String getPath() {
        return path;
    }

    public StatePersistenceConfigurationBuilder setPath(String path) {
        this.path = path;
        return this;
    }

    @Override
    public String getRelativeTo() {
        return relativeTo;
    }

    public StatePersistenceConfigurationBuilder setRelativeTo(String relativeTo) {
        this.relativeTo = relativeTo;
        return this;
    }

    @Override
    public StatePersistenceConfiguration getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }
}
