package org.infinispan.server.router.utils;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class CacheManagerTestingUtil {

    public static ConfigurationBuilder createDefaultCacheConfiguration() {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.compatibility().enable();
        return configurationBuilder;
    }

    public static GlobalConfigurationBuilder createDefaultGlobalConfiguration() {
       return new GlobalConfigurationBuilder();
    }
}
