package org.infinispan.server.router.configuration;

import java.net.InetAddress;

abstract class AbstractRouterConfiguration {

    private final int port;
    private final InetAddress ip;

    protected AbstractRouterConfiguration(InetAddress ip, int port) {
        this.port = port;
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public InetAddress getIp() {
        return ip;
    }

}
