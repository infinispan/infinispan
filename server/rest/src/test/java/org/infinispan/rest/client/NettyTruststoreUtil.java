package org.infinispan.rest.client;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;

import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.SslConfiguration;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.SslContextFactory;

import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * A small util for creating Netty's SSL Context
 *
 * @author Sebastian Łaskawiec
 */
public class NettyTruststoreUtil {

   public static SslContext createTruststoreContext(String truststore, char[] password, String... alpnProtocols) throws Exception {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(new FileInputStream(truststore), password);
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, password);

      SslProvider provider = OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK;
      return SslContextBuilder.forClient()
            .sslProvider(provider)
            .keyManager(kmf)
            .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocolConfig(new ApplicationProtocolConfig(
                  ApplicationProtocolConfig.Protocol.ALPN,
                  ApplicationProtocolConfig.SelectorFailureBehavior.CHOOSE_MY_LAST_PROTOCOL,
                  ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                  alpnProtocols))
            .build();
   }


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
            builder.keyManager(SslContextFactory.getKeyManagerFactory(
                  ssl.keyStoreFileName(),
                  ssl.keyStoreType(),
                  ssl.keyStorePassword(),
                  ssl.keyStoreCertificatePassword(),
                  ssl.keyAlias(),
                  NettyTruststoreUtil.class.getClassLoader()));
         }
         if (ssl.trustStoreFileName() != null) {
            builder.trustManager(SslContextFactory.getTrustManagerFactory(
                  ssl.trustStoreFileName(),
                  ssl.trustStoreType(),
                  ssl.trustStorePassword(),
                  NettyTruststoreUtil.class.getClassLoader()));
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
