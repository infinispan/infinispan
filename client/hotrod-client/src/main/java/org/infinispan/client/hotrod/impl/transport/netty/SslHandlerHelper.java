package org.infinispan.client.hotrod.impl.transport.netty;

import java.io.File;
import java.util.Collections;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.SslConfiguration;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.SslContextFactory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

public class SslHandlerHelper {

   public static SslHandler createSslHandler(Configuration configuration, ByteBufAllocator alloc, String... alpnProtocols) {
      SslConfiguration ssl = configuration.security().ssl();
      SslContext nettySslContext;
      SSLContext jdkSslContext = ssl.sslContext();
      if (jdkSslContext == null) {
         SslContextBuilder builder = SslContextBuilder.forClient();
         try {
            if (ssl.keyStoreFileName() != null) {
               builder.keyManager(SslContextFactory.getKeyManagerFactory(
                     ssl.keyStoreFileName(),
                     ssl.keyStoreType(),
                     ssl.keyStorePassword(),
                     ssl.keyStoreCertificatePassword(),
                     ssl.keyAlias(),
                     configuration.classLoader()));
            }
            if (ssl.trustStoreFileName() != null) {
               builder.trustManager(SslContextFactory.getTrustManagerFactory(
                     ssl.trustStoreFileName(),
                     ssl.trustStoreType(),
                     ssl.trustStorePassword(),
                     configuration.classLoader()));
            }
            if (ssl.trustStorePath() != null) {
               builder.trustManager(new File(ssl.trustStorePath()));
            }
            if (ssl.protocol() != null) {
               builder.protocols(ssl.protocol());
            }
            //Single Port ALPN negotiation part
            if (alpnProtocols != null && alpnProtocols.length > 0) {
               builder.sslProvider(OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK);
               builder.applicationProtocolConfig(new ApplicationProtocolConfig(
                     ApplicationProtocolConfig.Protocol.ALPN,
                     ApplicationProtocolConfig.SelectorFailureBehavior.CHOOSE_MY_LAST_PROTOCOL,
                     ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                     alpnProtocols));
            }
            nettySslContext = builder.build();
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      } else {
         nettySslContext = new JdkSslContext(jdkSslContext, true, ClientAuth.NONE);
      }
      SslHandler sslHandler = nettySslContext.newHandler(alloc, ssl.sniHostName(), -1);
      if (ssl.sniHostName() != null) {
         SSLParameters sslParameters = sslHandler.engine().getSSLParameters();
         sslParameters.setServerNames(Collections.singletonList(new SNIHostName(ssl.sniHostName())));
         sslHandler.engine().setSSLParameters(sslParameters);
      }
      return sslHandler;
   }

}
