package org.infinispan.server.core.utils;

import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.core.configuration.SslEngineConfiguration;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;

/**
 * SSL utils mainly for Netty.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class SslUtils {

   public static JdkSslContext createNettySslContext(SslConfiguration sslConfiguration, SslEngineConfiguration sslEngineConfiguration, ApplicationProtocolConfig alpnConfig) {
      return createSslContext(createJdkSslContext(sslConfiguration, sslEngineConfiguration), requireClientAuth(sslConfiguration), alpnConfig);
   }

   public static SSLContext createJdkSslContext(SslConfiguration sslConfiguration, SslEngineConfiguration sslEngineConfiguration) {
      if (sslEngineConfiguration.sslContext() != null) {
         return sslEngineConfiguration.sslContext();
      }
      return SslContextFactory.getContext(
            sslEngineConfiguration.keyStoreFileName(),
            sslEngineConfiguration.keyStoreType(),
            sslEngineConfiguration.keyStorePassword(),
            sslEngineConfiguration.keyStoreCertificatePassword(),
            sslEngineConfiguration.keyAlias(),
            sslEngineConfiguration.trustStoreFileName(),
            sslEngineConfiguration.trustStoreType(),
            sslEngineConfiguration.trustStorePassword(),
            sslEngineConfiguration.protocol(), null);
   }

   private static JdkSslContext createSslContext(SSLContext sslContext, ClientAuth clientAuth, ApplicationProtocolConfig alpnConfig) {
      SSLEngine engine = SslContextFactory.getEngine(sslContext, false, clientAuth == ClientAuth.REQUIRE);
      String[] ciphers = engine.getEnabledCipherSuites();
      return new JdkSslContext(sslContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, alpnConfig, clientAuth, null, false);
   }

   private static ClientAuth requireClientAuth(SslConfiguration sslConfig) {
      return sslConfig.requireClientAuth() ? ClientAuth.REQUIRE : ClientAuth.NONE;
   }
}
