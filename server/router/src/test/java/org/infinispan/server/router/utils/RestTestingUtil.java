package org.infinispan.server.router.utils;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.RestServer;

public class RestTestingUtil {

    public static RestServerConfigurationBuilder createDefaultRestConfiguration() {
        RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
        builder.startTransport(false);
        return builder;
    }

    public static RestServer createDefaultRestServer() {
        return createRest(createDefaultRestConfiguration(),
                CacheManagerTestingUtil.createDefaultGlobalConfiguration(),
                CacheManagerTestingUtil.createDefaultCacheConfiguration());
    }

    public static RestServer createRest(RestServerConfigurationBuilder configuration, GlobalConfigurationBuilder globalConfigurationBuilder, ConfigurationBuilder cacheConfigurationBuilder) {
        RestServer nettyRestServer = new RestServer();
        nettyRestServer.start(configuration.build(), new DefaultCacheManager(globalConfigurationBuilder.build(), cacheConfigurationBuilder.build()));
        return nettyRestServer;
    }
}
