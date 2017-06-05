package io.netty.handler.ssl;

import javax.net.ssl.SSLContext;

/**
 * Hacked ALPN SSL Context for Netty.
 *
 * @author Sebastian Łaskawiec
 */
public class AlpnHackedJdkSslContext extends JdkSslContext {

   public AlpnHackedJdkSslContext(SSLContext sslContext, boolean isClient, Iterable<String> ciphers, CipherSuiteFilter cipherFilter, ApplicationProtocolConfig apn, ClientAuth clientAuth) {
      super(sslContext, isClient, ciphers, cipherFilter, createNegotiator(apn), clientAuth, null, false);
   }

   private final static JdkApplicationProtocolNegotiator createNegotiator(ApplicationProtocolConfig apn) {
      if (apn == null) {
         return JdkDefaultApplicationProtocolNegotiator.INSTANCE;
      }
      return new AlpnHackedJdkApplicationProtocolNegotiator(apn.selectorFailureBehavior() == ApplicationProtocolConfig.SelectorFailureBehavior.FATAL_ALERT, apn.supportedProtocols());
   }
}
