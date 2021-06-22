package org.infinispan.server.core.utils;

import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.core.configuration.SslEngineConfiguration;
import org.infinispan.server.core.logging.Log;

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
      return createSslContext(createJdkSslContext(sslEngineConfiguration), requireClientAuth(sslConfiguration), alpnConfig);
   }

   public static SSLContext createJdkSslContext(SslEngineConfiguration sslEngineConfiguration) {
      if (sslEngineConfiguration.sslContext() != null) {
         return sslEngineConfiguration.sslContext();
      }
      return new SslContextFactory()
            .keyStoreFileName(sslEngineConfiguration.keyStoreFileName())
            .keyStoreType(sslEngineConfiguration.keyStoreType())
            .keyStorePassword(sslEngineConfiguration.keyStorePassword())
            .keyStoreCertificatePassword(sslEngineConfiguration.keyStoreCertificatePassword())
            .keyAlias(sslEngineConfiguration.keyAlias())
            .trustStoreFileName(sslEngineConfiguration.trustStoreFileName())
            .trustStoreType(sslEngineConfiguration.trustStoreType())
            .trustStorePassword(sslEngineConfiguration.trustStorePassword())
            .sslProtocol(sslEngineConfiguration.protocol())
            .provider(sslEngineConfiguration.provider())
            .getContext();
   }

   private static JdkSslContext createSslContext(SSLContext sslContext, ClientAuth clientAuth, ApplicationProtocolConfig alpnConfig) {
      SSLEngine engine = SslContextFactory.getEngine(sslContext, false, clientAuth == ClientAuth.REQUIRE);
      String[] ciphers = engine.getEnabledCipherSuites();
      if (Log.SECURITY.isDebugEnabled()) {
         Log.SECURITY.debugf("SSL Engine enabled cipher suites = %s", ciphers);
         Log.SECURITY.debugf("SSL Engine supported cipher suites = %s", engine.getSupportedCipherSuites());
         Log.SECURITY.debugf("SSL Engine enabled protocols = %s", engine.getEnabledProtocols());
         Log.SECURITY.debugf("SSL Engine supported protocols = %s", engine.getSupportedProtocols());
      }
      return new JdkSslContext(sslContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, alpnConfig, clientAuth, null, false);
   }

   private static ClientAuth requireClientAuth(SslConfiguration sslConfig) {
      return sslConfig.requireClientAuth() ? ClientAuth.REQUIRE : ClientAuth.NONE;
   }
}
