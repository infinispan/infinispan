package org.infinispan.server.jgroups.security;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;

import org.jboss.sasl.callback.DigestHashCallback;

/**
 * SaslClientCallbackHandler.
 *
 * @author Tristan Tarrant
 */
public class SaslClientCallbackHandler implements CallbackHandler {
    private final String realm;
    private final String name;
    private final String credential;

    public SaslClientCallbackHandler(String realm, String name, String credential) {
        this.realm = realm;
        this.name = name;
        this.credential = credential;
    }

    public SaslClientCallbackHandler(String name, String credential) {
        int realmSep = name.indexOf('@');
        this.realm = realmSep < 0 ? "" : name.substring(realmSep+1);
        this.name = realmSep < 0 ? name : name.substring(0, realmSep);
        this.credential = credential;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(credential.toCharArray());
            } else if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(name);
            } else if (callback instanceof RealmCallback) {
               ((RealmCallback) callback).setText(realm);
            } else if (callback instanceof DigestHashCallback) {
                ((DigestHashCallback) callback).setHexHash(credential);
            }
        }
    }

}
