package org.infinispan.server.security.http.localuser;

import java.security.Provider;

import org.kohsuke.MetaInfServices;
import org.wildfly.security.WildFlyElytronBaseProvider;

/**
 * Provider for the HTTP Local authentication mechanism.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@MetaInfServices(Provider.class)
public final class WildFlyElytronHttpLocalUserProvider extends WildFlyElytronBaseProvider {

   private static WildFlyElytronHttpLocalUserProvider INSTANCE = new WildFlyElytronHttpLocalUserProvider();

   /**
    * Construct a new instance.
    */
   public WildFlyElytronHttpLocalUserProvider() {
      super("WildFlyElytronHttpLocalUserProvider", "1.0", "WildFly Elytron HTTP LOCALUSER Provider");
      putService(new ProviderService(this, HTTP_SERVER_FACTORY_TYPE, "LOCALUSER", "org.infinispan.server.security.http.localuser.LocalUserMechanismFactory", emptyList, emptyMap, true, true));
   }

   /**
    * Get the HTTP BASIC authentication mechanism provider instance.
    *
    * @return the HTTP BASIC authentication mechanism provider instance
    */
   public static WildFlyElytronHttpLocalUserProvider getInstance() {
      return INSTANCE;
   }

}
