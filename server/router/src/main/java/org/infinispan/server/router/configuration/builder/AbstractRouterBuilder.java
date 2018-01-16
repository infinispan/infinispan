package org.infinispan.server.router.configuration.builder;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.router.logging.RouterLogger;

public abstract class AbstractRouterBuilder implements ConfigurationBuilderParent {

    protected static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

    protected final ConfigurationBuilderParent parent;
    protected int port;
    protected InetAddress ip;
    protected boolean enabled;

    protected AbstractRouterBuilder(ConfigurationBuilderParent parent) {
        this.parent = parent;
    }

    public AbstractRouterBuilder port(int port) {
        this.port = port;
        return this;
    }

    public AbstractRouterBuilder ip(InetAddress ip) {
        this.ip = ip;
        return this;
    }

    public AbstractRouterBuilder enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    protected void validate() {
        if (this.enabled) {
            if (ip == null) {
                throw new IllegalArgumentException("IP can not be null");
            }
            if (port < 0) {
                throw new IllegalArgumentException("Port can not be negative");
            }
        }
    }

    @Override
    public RoutingBuilder routing() {
        return parent.routing();
    }

    @Override
    public HotRodRouterBuilder hotrod() {
        return parent.hotrod();
    }

    @Override
    public RestRouterBuilder rest() {
        return parent.rest();
    }

    @Override
    public SinglePortRouterBuilder singlePort() {
        return parent.singlePort();
    }
}
