package org.infinispan.hotrod.impl.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;

/**
 *
 * No-op {@link javax.security.auth.callback.CallbackHandler}. Convenient CallbackHandler which comes handy when
 * no auth. callback is needed. This applies namely to SASL EXTERNAL auth. mechanism when auth. information is obtained
 * from external channel, like TLS certificate.
 *
 * @since 14.0
 */
public class VoidCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) {
        // NO-OP
    }
}
