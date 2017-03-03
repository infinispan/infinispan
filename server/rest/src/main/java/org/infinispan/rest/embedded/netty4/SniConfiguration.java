package org.infinispan.rest.embedded.netty4;

import java.util.Arrays;

import javax.net.ssl.SSLContext;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.util.DomainNameMapping;
import io.netty.util.DomainNameMappingBuilder;

/**
 * TLS/SSL Server Name Indication configuration.
 *
 * @author Sebastian Łaskawiec
 * @see https://tools.ietf.org/html/rfc6066#page-6
 * Temporary fork from RestEasy 3.1.0
 */
public class SniConfiguration {

   protected final DomainNameMappingBuilder<SslContext> mapping;

   /**
    * Constructs new {@link SniConfiguration}.
    *
    * @param defaultServerKeystore default keystore to be used when no SNI is specified by the client.
    */
   public SniConfiguration(SSLContext defaultServerKeystore) {
      mapping = new DomainNameMappingBuilder<>(createContext(defaultServerKeystore));
   }

   /**
    * Adds SNI mapping.
    *
    * @param sniHostName SNI Host Name from TLS Extensions.
    * @param sslContext  SSLContext to be associated with given SNI Host Name.
    * @return <code>this</code> configuration.
    */
   public SniConfiguration addSniMapping(String sniHostName, SSLContext sslContext) {
      mapping.add(sniHostName, createContext(sslContext));
      return this;
   }

   protected DomainNameMapping<SslContext> buildMapping() {
      return mapping.build();
   }

   private SslContext createContext(SSLContext sslContext) {
      //Unfortunately we need to grap a list of available ciphers from the engine.
      //If we won't, JdkSslContext will use common ciphers from DEFAULT and SUPPORTED, which gives us 5 out of ~50 available ciphers
      //Of course, we don't need to any specific engine configuration here... just a list of ciphers
      String[] ciphers = sslContext.createSSLEngine().getSupportedCipherSuites();
      return new JdkSslContext(sslContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, null,
            ClientAuth.OPTIONAL);
   }

}
