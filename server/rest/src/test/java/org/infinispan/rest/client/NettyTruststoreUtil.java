package org.infinispan.rest.client;

import java.io.File;

import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.SslConfiguration;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.SslContextFactory;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * A small util for creating Netty's SSL Context
 *
 * @author Sebastian Łaskawiec
 */
public class NettyTruststoreUtil {
   public static SslContext createSslContext(RestClientConfiguration configuration) {
      SslConfiguration ssl = configuration.security().ssl();
      if (!ssl.enabled()) {
         return null;
      }
      if (ssl.sslContext() != null) {
         return new JdkSslContext(ssl.sslContext(), true, ClientAuth.NONE);
      }
      SslContextBuilder builder = SslContextBuilder.forClient();

      try {
         if (ssl.keyStoreFileName() != null) {
            builder.keyManager(new SslContextFactory()
                  .keyStoreFileName(ssl.keyStoreFileName())
                  .keyStorePassword(ssl.keyStorePassword())
                  .keyStoreCertificatePassword(ssl.keyStoreCertificatePassword())
                  .keyStoreType(ssl.keyStoreType())
                  .keyAlias(ssl.keyAlias())
                  .classLoader(NettyTruststoreUtil.class.getClassLoader())
                  .getKeyManagerFactory());
         }
         if (ssl.trustStoreFileName() != null) {
            builder.trustManager(new SslContextFactory()
                  .trustStoreFileName(ssl.trustStoreFileName())
                  .trustStorePassword(ssl.trustStorePassword())
                  .trustStoreType(ssl.trustStoreType())
                  .classLoader(NettyTruststoreUtil.class.getClassLoader())
                  .getTrustManagerFactory());
         }
         if (ssl.trustStorePath() != null) {
            builder.trustManager(new File(ssl.trustStorePath()));
         }
         if (ssl.protocol() != null) {
            builder.protocols(ssl.protocol());
         }
         if (configuration.protocol() == Protocol.HTTP_20) {
            builder.applicationProtocolConfig(new ApplicationProtocolConfig(
                  ApplicationProtocolConfig.Protocol.ALPN,
                  ApplicationProtocolConfig.SelectorFailureBehavior.CHOOSE_MY_LAST_PROTOCOL,
                  ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                  ApplicationProtocolNames.HTTP_2));
         }
         return builder.build();
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }
}
