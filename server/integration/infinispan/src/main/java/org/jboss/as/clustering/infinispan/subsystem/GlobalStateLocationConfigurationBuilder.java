package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.globalstate.ConfigurationStorage;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.GlobalStateLocationConfiguration;
import org.jboss.msc.value.Value;

/**
 * GlobalStateLocationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStateLocationConfigurationBuilder implements Value<GlobalStateLocationConfiguration>, GlobalStateLocationConfiguration {
    private String persistencePath;
    private String persistenceRelativeTo;
    private String temporaryPath;
    private String temporaryRelativeTo;
    private ConfigurationStorage configurationStorage;
    private String configurationStorageClass;

    @Override
    public String getPersistencePath() {
        return persistencePath;
    }

    public GlobalStateLocationConfigurationBuilder setPersistencePath(String path) {
        this.persistencePath = path;
        return this;
    }

    @Override
    public String getPersistenceRelativeTo() {
        return persistenceRelativeTo;
    }

    public GlobalStateLocationConfigurationBuilder setPersistenceRelativeTo(String relativeTo) {
        this.persistenceRelativeTo = relativeTo;
        return this;
    }

    public GlobalStateLocationConfigurationBuilder setTemporaryPath(String path) {
        this.temporaryPath = path;
        return this;
    }

    @Override
    public String getTemporaryPath() {
        return temporaryPath;
    }

    public GlobalStateLocationConfigurationBuilder setTemporaryRelativeTo(String relativeTo) {
        this.temporaryRelativeTo = relativeTo;
        return this;
    }

    @Override
    public String getTemporaryRelativeTo() {
        return temporaryRelativeTo;
    }

    public GlobalStateLocationConfigurationBuilder setConfigurationStorage(ConfigurationStorage configurationStorage) {
        this.configurationStorage = configurationStorage;
        return this;
    }

    @Override
    public ConfigurationStorage getConfigurationStorage() {
        return configurationStorage;
    }

    public GlobalStateLocationConfigurationBuilder setConfigurationStorageClass(String configurationStorageClass) {
        this.configurationStorageClass = configurationStorageClass;
        return this;
    }

    @Override
    public String getConfigurationStorageClass() {
        return configurationStorageClass;
    }

    @Override
    public GlobalStateLocationConfiguration getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }
}
