package io.netty.handler.ssl;


import java.util.List;

import javax.net.ssl.SSLEngine;

/**
 * Netty's negotiator for Hacked ALPN SSL Engine.
 *
 * @author Sebastian Łaskawiec
 */
public class AlpnHackedJdkApplicationProtocolNegotiator extends JdkBaseApplicationProtocolNegotiator {

   private static final SslEngineWrapperFactory ALPN_WRAPPER = new SslEngineWrapperFactory() {
      @Override
      public SSLEngine wrapSslEngine(SSLEngine engine, JdkApplicationProtocolNegotiator applicationNegotiator,
                                     boolean isServer) {
         ALPNHackSSLEngine wrappedEngine = new ALPNHackSSLEngine(engine);
         wrappedEngine.setApplicationProtocols(applicationNegotiator.protocols());
         return new AlpnHackedJdkSslEngine(wrappedEngine);
      }
   };

   public AlpnHackedJdkApplicationProtocolNegotiator(boolean failIfNoCommonProtocols, List<String> protocols) {
      super(ALPN_WRAPPER
            , failIfNoCommonProtocols ? FAIL_SELECTOR_FACTORY : NO_FAIL_SELECTOR_FACTORY
            , failIfNoCommonProtocols ? FAIL_SELECTION_LISTENER_FACTORY : NO_FAIL_SELECTION_LISTENER_FACTORY
            , protocols);
   }

}
