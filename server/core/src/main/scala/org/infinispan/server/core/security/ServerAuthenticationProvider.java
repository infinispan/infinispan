package org.infinispan.server.core.security;

import java.util.Map;


/**
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 * @author Tristan Tarrant
 */
public interface ServerAuthenticationProvider {

   /**
    * Get a callback handler for the given mechanism name.
    *
    * This method is called each time a mechanism is selected for the connection and the resulting
    * AuthorizingCallbackHandler will be cached and used multiple times for this connection,
    * AuthorizingCallbackHandler should either be thread safe or the ServerAuthenticationProvider
    * should provide a new instance each time called.
    *
    * @param mechanismName
    *           the SASL mechanism to get a callback handler for
    * @param mechanismProperties
    *           the mechanism properties that might need to be adjusted to support the specific mechanism / callbackhandler combination
    * @return the callback handler or {@code null} if the mechanism is not supported
    */
   AuthorizingCallbackHandler getCallbackHandler(String mechanismName, Map<String, String> mechanismProperties);

}
