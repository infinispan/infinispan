package org.infinispan.client.hotrod.security;

import java.security.Provider;
import java.util.Arrays;

import org.infinispan.client.hotrod.logging.Log;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
public class ElytronSaslProviders {
   /**
    * Install the Elytron SASL security providers. Should be invoked before constructing an authentication-enabled
    * RemoteCacheManager
    */
   public static void installSecurityProviders() {
      // Register only the providers that matter to us
      for (String name : Arrays.asList(
            "org.wildfly.security.sasl.plain.WildFlyElytronSaslPlainProvider",
            "org.wildfly.security.sasl.digest.WildFlyElytronSaslDigestProvider",
            "org.wildfly.security.sasl.external.WildFlyElytronSaslExternalProvider",
            "org.wildfly.security.sasl.oauth2.WildFlyElytronSaslOAuth2Provider",
            "org.wildfly.security.sasl.scram.WildFlyElytronSaslScramProvider",
            "org.wildfly.security.sasl.gssapi.WildFlyElytronSaslGssapiProvider",
            "org.wildfly.security.sasl.gs2.WildFlyElytronSaslGs2Provider"
      )) {
         try {
            Provider provider = (Provider) Class.forName(name).getConstructor(new Class[]{}).newInstance(new Object[]{});
            SecurityActions.addSecurityProvider(provider);
         } catch (Throwable t) {
            Log.HOTROD.debugf(t, "Could not initialize Elytron SASL provider '%s'", name);
         }
      }

   }
}
