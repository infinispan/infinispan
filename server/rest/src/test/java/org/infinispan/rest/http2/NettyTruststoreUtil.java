package org.infinispan.rest.http2;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;

import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
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

}
