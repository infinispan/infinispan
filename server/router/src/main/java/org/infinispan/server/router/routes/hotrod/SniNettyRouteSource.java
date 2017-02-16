package org.infinispan.server.router.routes.hotrod;

import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.router.router.impl.hotrod.handlers.util.SslUtils;
import org.infinispan.server.router.routes.SniRouteSource;

import io.netty.handler.ssl.SslContext;

public class SniNettyRouteSource implements SniRouteSource {

    private final SslContext nettyContext;
    private final SSLContext jdkContext;
    private final String sniHostName;

    public SniNettyRouteSource(String sniHostName, SSLContext sslContext) {
        this.sniHostName = sniHostName;
        this.jdkContext = sslContext;
        nettyContext = SslUtils.INSTANCE.toNettySslContext(Optional.ofNullable(jdkContext));
    }

    public SniNettyRouteSource(String sniHostName, String keyStoreFileName, char[] keyStorePassword) {
        this(sniHostName, SslContextFactory.getContext(keyStoreFileName, keyStorePassword, null, null));
    }

    @Override
    public SSLContext getSslContext() {
        return jdkContext;
    }

    @Override
    public String getSniHostName() {
        return sniHostName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SniNettyRouteSource that = (SniNettyRouteSource) o;

        if (!getSniHostName().equals(that.getSniHostName())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return getSniHostName().hashCode();
    }

    @Override
    public String toString() {
        return "SniNettyRouteSource{" +
                "sniHostName='" + sniHostName + '\'' +
                '}';
    }

    @Override
    public void validate() {
        if (sniHostName == null || "".equals(sniHostName)) {
            throw new IllegalArgumentException("SNI Host name can not be null");
        }
        if (jdkContext == null) {
            throw new IllegalArgumentException("JDK SSL Context must not be null");
        }
    }
}
