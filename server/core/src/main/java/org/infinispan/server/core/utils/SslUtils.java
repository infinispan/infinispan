package org.infinispan.server.core.utils;

import java.util.Arrays;

import javax.net.ssl.SSLContext;

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
            .getContext();
   }

   private static JdkSslContext createSslContext(SSLContext sslContext, ClientAuth clientAuth, ApplicationProtocolConfig alpnConfig) {
      //Unfortunately we need to grab a list of available ciphers from the engine.
      //If we won't, JdkSslContext will use common ciphers from DEFAULT and SUPPORTED, which gives us 5 out of ~50 available ciphers
      //Of course, we don't need to any specific engine configuration here... just a list of ciphers
      String[] ciphers = SslContextFactory.getEngine(sslContext, false, clientAuth == ClientAuth.REQUIRE).getSupportedCipherSuites();
      return new JdkSslContext(sslContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, alpnConfig, clientAuth, null, false);
   }

   private static ClientAuth requireClientAuth(SslConfiguration sslConfig) {
      return sslConfig.requireClientAuth() ? ClientAuth.REQUIRE : ClientAuth.NONE;
   }
}
