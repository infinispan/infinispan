package org.infinispan.server.router.router.impl.hotrod.handlers.util;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.util.SslContextFactory;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;

/**
 * A simple utility class for managing SSL Contexts
 *
 * @author Sebastian Łaskawiec
 */
public enum SslUtils {
    INSTANCE;

    /**
     * Creates Netty's {@link SslContext} based on optional standard JDK's {@link SSLContext}. If {@link
     * Optional#empty()} is passed as an argument, this method will return the default {@link SslContext}.
     *
     * @param context Optional {@link SSLContext}.
     * @return Netty's {@link SslContext}.
     */
    public SslContext toNettySslContext(Optional<SSLContext> context) {
        try {
            SSLContext jdkContext = context.orElse(SSLContext.getDefault());
            String[] ciphers = SslContextFactory.getEngine(jdkContext, false, false).getSupportedCipherSuites();
            return new JdkSslContext(jdkContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, null, ClientAuth.OPTIONAL);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
