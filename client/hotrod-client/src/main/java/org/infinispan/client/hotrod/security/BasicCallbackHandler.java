package org.infinispan.client.hotrod.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

/**
 * A basic {@link CallbackHandler}. This can be used for PLAIN and CRAM-MD mechanisms
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class BasicCallbackHandler implements CallbackHandler {
    private final String username;
    private final String realm;
    private final char[] password;

    public BasicCallbackHandler() {
        this(null, null, null);
    }

    public BasicCallbackHandler(String username, char[] password) {
        this(username, null, password);
    }

    public BasicCallbackHandler(String username, String realm, char[] password) {
        this.username = username;
        this.password = password;
        this.realm = realm;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nameCallback = (NameCallback) callback;
                nameCallback.setName(username);
            } else if (callback instanceof PasswordCallback) {
                PasswordCallback passwordCallback = (PasswordCallback) callback;
                passwordCallback.setPassword(password);
            } else if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
                authorizeCallback.setAuthorized(authorizeCallback.getAuthenticationID().equals(
                      authorizeCallback.getAuthorizationID()));
            } else if (callback instanceof RealmCallback) {
                if (realm == null)
                    throw new UnsupportedCallbackException(callback, "The mech requests a realm, but none has been supplied");
                RealmCallback realmCallback = (RealmCallback) callback;
                realmCallback.setText(realm);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
