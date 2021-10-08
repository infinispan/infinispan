package org.infinispan.server.router.utils;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.core.DummyServerManagement;
import org.infinispan.server.router.Router;

public class RestTestingUtil {

    public static RestServerConfigurationBuilder createDefaultRestConfiguration() {
        RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
        builder.startTransport(false);
        return builder;
    }

    public static RestServer createDefaultRestServer(String ctx, String... definedCaches) {
        return createRest(ctx, createDefaultRestConfiguration(), new GlobalConfigurationBuilder(),
              new ConfigurationBuilder(), definedCaches);
    }

    public static RestServer createRest(String ctx, RestServerConfigurationBuilder configuration, GlobalConfigurationBuilder globalConfigurationBuilder, ConfigurationBuilder cacheConfigurationBuilder, String... definedCaches) {
      configuration.contextPath(ctx);
        RestServer nettyRestServer = new RestServer();
        Configuration cacheConfiguration = cacheConfigurationBuilder.build();
        DefaultCacheManager cacheManager = new DefaultCacheManager(globalConfigurationBuilder.build());
        for (String cache : definedCaches) {
            cacheManager.defineConfiguration(cache, cacheConfiguration);
        }
        nettyRestServer.setServerManagement(new DummyServerManagement(), true);
        nettyRestServer.start(configuration.build(), cacheManager);
        return nettyRestServer;
    }

    public static void killRouter(Router router) {
        if (router != null) {
            router.stop();
        }
    }
}
