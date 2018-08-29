package org.infinispan.server.router.utils;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;

public class RestTestingUtil {

    public static RestServerConfigurationBuilder createDefaultRestConfiguration() {
        RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
        builder.startTransport(false);
        return builder;
    }

    public static RestServer createDefaultRestServer(String... definedCaches) {
        return createRest(createDefaultRestConfiguration(), new GlobalConfigurationBuilder(),
              new ConfigurationBuilder(), definedCaches);
    }

    public static RestServer createRest(RestServerConfigurationBuilder configuration, GlobalConfigurationBuilder globalConfigurationBuilder, ConfigurationBuilder cacheConfigurationBuilder, String... definedCaches) {
        RestServer nettyRestServer = new RestServer();
        Configuration cacheConfiguration = cacheConfigurationBuilder.build();
        DefaultCacheManager cacheManager = new DefaultCacheManager(globalConfigurationBuilder.build(), cacheConfiguration);
        for (String cache : definedCaches) {
            cacheManager.defineConfiguration(cache, cacheConfiguration);
        }
        nettyRestServer.start(configuration.build(), cacheManager);
        return nettyRestServer;
    }
}
