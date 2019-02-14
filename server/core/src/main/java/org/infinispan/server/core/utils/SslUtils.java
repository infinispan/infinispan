package org.infinispan.server.core.utils;

import java.util.Arrays;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.core.configuration.SslEngineConfiguration;

import io.netty.handler.ssl.AlpnHackedJdkSslContext;
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
      //Unfortunately we need to grab a list of available ciphers from the engine.
      //If we won't, JdkSslContext will use common ciphers from DEFAULT and SUPPORTED, which gives us 5 out of ~50 available ciphers
      //Of course, we don't need to any specific engine configuration here... just a list of ciphers
      String[] ciphers = SslContextFactory.getEngine(sslContext, false, clientAuth == ClientAuth.REQUIRE).getSupportedCipherSuites();
      if (alpnConfig != null && !isJdkAlpn()) {
         //we want to minimize the impact of possibly bugs in hacked SSL Context.
         return new AlpnHackedJdkSslContext(sslContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, alpnConfig, clientAuth);
      } else {
         return new JdkSslContext(sslContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, alpnConfig, clientAuth);
      }
   }

   private static ClientAuth requireClientAuth(SslConfiguration sslConfig) {
      return sslConfig.requireClientAuth() ? ClientAuth.REQUIRE : ClientAuth.NONE;
   }

   private static boolean isJdkAlpn() {
      return !SecurityActions.getSystemProperty("java.version").startsWith("1.8");
   }
}
