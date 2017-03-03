package org.infinispan.server.router.utils;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.embedded.netty4.NettyRestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;

public class RestTestingUtil {

    public static RestServerConfigurationBuilder createDefaultRestConfiguration() {
        RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
        builder.startTransport(false);
        return builder;
    }

    public static NettyRestServer createDefaultRestServer() {
        return createRest(createDefaultRestConfiguration(),
                CacheManagerTestingUtil.createDefaultGlobalConfiguration(),
                CacheManagerTestingUtil.createDefaultCacheConfiguration());
    }

    public static NettyRestServer createRest(RestServerConfigurationBuilder configuration, GlobalConfigurationBuilder globalConfigurationBuilder, ConfigurationBuilder cacheConfigurationBuilder) {
        NettyRestServer nettyRestServer = NettyRestServer.createServer(createDefaultRestConfiguration().build(), new DefaultCacheManager(globalConfigurationBuilder.build(), cacheConfigurationBuilder.build()));
        nettyRestServer.start();
        return nettyRestServer;
    }
}
