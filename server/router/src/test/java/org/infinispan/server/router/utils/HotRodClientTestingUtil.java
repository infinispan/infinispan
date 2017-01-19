package org.infinispan.server.router.utils;

import java.net.InetAddress;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

public class HotRodClientTestingUtil {

    public static RemoteCacheManager createWithSni(InetAddress ip, int port, String sniHostName, String trustorePath, char[] password) {
        ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        clientBuilder = withIpAndPort(clientBuilder, ip, port);
        clientBuilder = withSni(clientBuilder, sniHostName, trustorePath, password);
        clientBuilder = withSingleConnection(clientBuilder);
        return new RemoteCacheManager(clientBuilder.build());
    }

    public static RemoteCacheManager createWithSsl(InetAddress ip, int port, String trustorePath, char[] password) {
        ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        clientBuilder = withIpAndPort(clientBuilder, ip, port);
        clientBuilder = withSsl(clientBuilder, trustorePath, password);
        clientBuilder = withSingleConnection(clientBuilder);
        return new RemoteCacheManager(clientBuilder.build());
    }

    public static RemoteCacheManager createNoAuth(InetAddress ip, int port) {
        ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        clientBuilder = withIpAndPort(clientBuilder, ip, port);
        clientBuilder = withSingleConnection(clientBuilder);
        return new RemoteCacheManager(clientBuilder.build());
    }

    private static ConfigurationBuilder withIpAndPort(ConfigurationBuilder cb, InetAddress ip, int port) {
        cb.addServer()
                .host(ip.getHostAddress())
                .port(port);
        return cb;
    }

    private static ConfigurationBuilder withSsl(ConfigurationBuilder cb, String trustorePath, char[] password) {
        cb.security()
                .ssl()
                .enabled(true)
                .trustStoreFileName(trustorePath)
                .trustStorePassword(password);
        return cb;
    }

    private static ConfigurationBuilder withSingleConnection(ConfigurationBuilder cb) {
        cb.maxRetries(0);
        return cb;
    }

    private static ConfigurationBuilder withSni(ConfigurationBuilder cb, String sniHostName, String trustorePath, char[] password) {
        cb = withSsl(cb, trustorePath, password);
        cb.security().ssl().sniHostName(sniHostName);
        return cb;
    }
}
